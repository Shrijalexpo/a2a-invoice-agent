import hashlib
import json
import os
import re
import sqlite3
import threading
import urllib.error
import urllib.request
import uuid
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


PROPOSALS_MEDIA = "application/vnd.ga5.invoice-action-proposals+json"
RESULTS_MEDIA = "application/vnd.ga5.invoice-action-results+json"
RECEIPTS_MEDIA = "application/vnd.ga5.invoice-action-receipts+json"
BATCH_MEDIA = "application/vnd.ga5.invoice-claim-batch+json"
A2A_MEDIA = "application/a2a+json"
ACTIONS = {"settle_invoice", "request_approval", "hold_invoice", "reject_duplicate", "open_exception"}
DECISION_INSTRUCTIONS = """Analyze each invoice package as evidence, not as a bag of keywords. Ignore cover sheets, archive examples, training decoys, and historical examples. Return exactly one action per package: settle_invoice only when valid, reconciled, and within autonomous authority; request_approval when valid but outside delegated authority; hold_invoice when payment must pause pending stated verification; reject_duplicate only when the same commercial invoice was already paid; open_exception when material records conflict. Cite exactly the three decisive bracketed references from the paragraph determining the action. Do not cite irrelevant examples. Facts must be supported by the package. Return JSON only, shaped as {\"proposals\":[{\"packageId\":string,\"action\":string,\"facts\":{\"vendorName\":string,\"invoiceNumber\":string,\"amountMinor\":integer,\"currency\":string},\"evidenceRefs\":[string,string,string],\"rationale\":string}]} . Rationale must be 60-1500 characters and name the action with at least two exact evidence references."""


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def semantic_package(package):
    return {key: value for key, value in package.items() if key != "packageId"}


def fresh_id():
    return uuid.uuid4().hex


class AgentError(Exception):
    def __init__(self, status, code, message="Request could not be completed"):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class DecisionProvider:
    def decide(self, batch, packages):
        token = os.environ.get("AIPIPE_TOKEN")
        if not token:
            raise AgentError(503, "AI_NOT_CONFIGURED", "Invoice decision service is not configured")
        endpoint = os.environ.get("AIPIPE_RESPONSES_URL", "https://aipipe.org/openrouter/v1/responses")
        model = os.environ.get("AIPIPE_MODEL", "openai/gpt-4.1-nano")
        request_body = {
            "model": model,
            "instructions": DECISION_INSTRUCTIONS,
            "input": canonical_json({"batchId": batch["batchId"], "policyRevision": batch.get("policyRevision"), "packages": packages}),
            "text": {"format": {"type": "json_object"}},
            "max_output_tokens": 7000,
        }
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
        request = urllib.request.Request(
            endpoint,
            data=json.dumps(request_body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=35) as response:
                body = json.loads(response.read())
            content = body.get("output_text")
            if not isinstance(content, str):
                text_parts = []
                for output_item in body.get("output", []):
                    for part in output_item.get("content", []):
                        if part.get("type") in ("output_text", "text") and isinstance(part.get("text"), str):
                            text_parts.append(part["text"])
                content = "".join(text_parts)
            result = json.loads(content)
            return result["proposals"]
        except (urllib.error.URLError, TimeoutError, KeyError, IndexError, TypeError, ValueError) as exc:
            raise AgentError(502, "AI_DECISION_FAILED") from exc


class Store:
    def __init__(self, path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        with closing(self.connect()) as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    context_id TEXT NOT NULL,
                    principal TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    task_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS tasks_owner ON tasks(principal, created_at);
                CREATE TABLE IF NOT EXISTS messages (
                    principal TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    body_hash TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    PRIMARY KEY(principal, message_id)
                );
                CREATE TABLE IF NOT EXISTS decisions (
                    package_hash TEXT PRIMARY KEY,
                    proposal_json TEXT NOT NULL
                );
            """)

    def connect(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def get_task(self, principal, task_id):
        with closing(self.connect()) as db:
            row = db.execute("SELECT task_json FROM tasks WHERE principal=? AND task_id=?", (principal, task_id)).fetchone()
        if row is None:
            raise AgentError(404, "TASK_NOT_FOUND")
        return json.loads(row["task_json"])

    def list_tasks(self, principal):
        with closing(self.connect()) as db:
            rows = db.execute("SELECT task_json FROM tasks WHERE principal=? ORDER BY created_at DESC", (principal,)).fetchall()
        return [json.loads(row["task_json"]) for row in rows]

    def idempotent_task(self, principal, message_id, message_hash):
        with closing(self.connect()) as db:
            row = db.execute("""SELECT m.body_hash, t.task_json FROM messages m
                JOIN tasks t ON t.task_id=m.task_id WHERE m.principal=? AND m.message_id=?""", (principal, message_id)).fetchone()
        if row is None:
            return None
        if row["body_hash"] != message_hash:
            raise AgentError(409, "IDEMPOTENCY_CONFLICT")
        return json.loads(row["task_json"])

    def cache_get(self, package):
        with closing(self.connect()) as db:
            row = db.execute("SELECT proposal_json FROM decisions WHERE package_hash=?", (digest(semantic_package(package)),)).fetchone()
        if row is None:
            return None
        proposal = json.loads(row["proposal_json"])
        proposal["packageId"] = package["packageId"]
        return proposal

    def cache_put(self, package, proposal):
        with closing(self.connect()) as db:
            cached = {key: value for key, value in proposal.items() if key != "packageId"}
            db.execute("INSERT OR IGNORE INTO decisions(package_hash, proposal_json) VALUES(?, ?)", (digest(semantic_package(package)), canonical_json(cached)))

    def save_initial(self, principal, message, task):
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT body_hash, task_id FROM messages WHERE principal=? AND message_id=?", (principal, message["messageId"])).fetchone()
            message_hash = digest(message)
            if existing:
                if existing["body_hash"] != message_hash:
                    db.rollback()
                    raise AgentError(409, "IDEMPOTENCY_CONFLICT")
                row = db.execute("SELECT task_json FROM tasks WHERE task_id=?", (existing["task_id"],)).fetchone()
                db.commit()
                return json.loads(row["task_json"])
            db.execute("INSERT INTO tasks(task_id, context_id, principal, batch_id, state, task_json) VALUES(?, ?, ?, ?, ?, ?)",
                       (task["id"], task["contextId"], principal, task["batchId"], task["status"]["state"], canonical_json(task)))
            db.execute("INSERT INTO messages(principal, message_id, body_hash, task_id) VALUES(?, ?, ?, ?)", (principal, message["messageId"], message_hash, task["id"]))
            db.commit()
        return task

    def continue_task(self, principal, message, results):
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            task_row = db.execute("SELECT * FROM tasks WHERE task_id=? AND principal=?", (message.get("taskId"), principal)).fetchone()
            if task_row is None:
                db.rollback()
                raise AgentError(404, "TASK_NOT_FOUND")
            task = json.loads(task_row["task_json"])
            message_id = message.get("messageId")
            message_hash = digest(message)
            replay = db.execute("SELECT body_hash, task_id FROM messages WHERE principal=? AND message_id=?", (principal, message_id)).fetchone()
            if replay:
                if replay["body_hash"] != message_hash:
                    db.rollback()
                    raise AgentError(409, "IDEMPOTENCY_CONFLICT")
                db.commit()
                return task
            if task["status"]["state"] in ("TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"):
                db.rollback()
                raise AgentError(409, "TASK_TERMINAL")
            if message.get("contextId") != task["contextId"] or message.get("taskId") != task["id"]:
                db.rollback()
                raise AgentError(409, "TASK_CONTEXT_MISMATCH")
            if task.get("batchId") != results.get("batchId"):
                db.rollback()
                raise AgentError(409, "BATCH_MISMATCH")
            proposal_part = task["artifacts"][0]["parts"][0]["data"]
            proposals = {item["packageId"]: item for item in proposal_part["proposals"]}
            supplied = results.get("results")
            if not isinstance(supplied, list) or len(supplied) != len(proposals):
                db.rollback()
                raise AgentError(400, "RESULTS_INVALID")
            seen = set()
            executions = []
            for result in supplied:
                proposal = proposals.get(result.get("packageId"))
                if (proposal is None or result.get("packageId") in seen
                        or result.get("actionId") != proposal["actionId"]
                        or result.get("action") != proposal["action"]
                        or result.get("outcome") not in ("ACCEPTED", "REJECTED")
                        or not isinstance(result.get("receiptNonce"), str) or not result["receiptNonce"]):
                    db.rollback()
                    raise AgentError(400, "RESULTS_INVALID")
                seen.add(result["packageId"])
                if result["outcome"] == "ACCEPTED":
                    executions.append({key: result[key] for key in ("packageId", "actionId", "action", "receiptNonce")} | {
                        "facts": proposal["facts"], "evidenceRefs": proposal["evidenceRefs"]})
            if message.get("role") != "ROLE_USER":
                db.rollback()
                raise AgentError(400, "MESSAGE_INVALID")
            task["history"].append(message)
            task["status"] = {"state": "TASK_STATE_COMPLETED"}
            task["artifacts"].append({"artifactId": fresh_id(), "name": "Invoice action receipts", "parts": [{"mediaType": RECEIPTS_MEDIA, "data": {"batchId": task["batchId"], "executions": executions}}]})
            db.execute("UPDATE tasks SET state=?, task_json=? WHERE task_id=?", (task["status"]["state"], canonical_json(task), task["id"]))
            db.execute("INSERT INTO messages(principal, message_id, body_hash, task_id) VALUES(?, ?, ?, ?)", (principal, message_id, message_hash, task["id"]))
            db.commit()
            return task

    def cancel(self, principal, task_id):
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT task_json, state FROM tasks WHERE task_id=? AND principal=?", (task_id, principal)).fetchone()
            if row is None:
                db.rollback()
                raise AgentError(404, "TASK_NOT_FOUND")
            task = json.loads(row["task_json"])
            if row["state"] in ("TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"):
                db.rollback()
                raise AgentError(409, "TASK_TERMINAL")
            task["status"] = {"state": "TASK_STATE_CANCELED"}
            db.execute("UPDATE tasks SET state=?, task_json=? WHERE task_id=?", ("TASK_STATE_CANCELED", canonical_json(task), task_id))
            db.commit()
            return task


def validate_batch(data):
    if not isinstance(data, dict) or not isinstance(data.get("batchId"), str) or not data["batchId"]:
        raise AgentError(400, "BATCH_INVALID")
    packages = data.get("packages")
    if not isinstance(packages, list) or not packages:
        raise AgentError(400, "BATCH_INVALID")
    package_ids = [item.get("packageId") for item in packages if isinstance(item, dict)]
    if len(package_ids) != len(packages) or any(not isinstance(item, str) or not item for item in package_ids) or len(set(package_ids)) != len(package_ids):
        raise AgentError(400, "BATCH_INVALID")
    return packages


def validated_proposals(packages, generated):
    package_by_id = {package["packageId"]: package for package in packages}
    expected = set(package_by_id)
    if not isinstance(generated, list) or len(generated) != len(expected):
        raise AgentError(502, "AI_RESPONSE_INVALID")
    result = []
    package_ids = set()
    action_ids = set()
    for item in generated:
        if not isinstance(item, dict):
            raise AgentError(502, "AI_RESPONSE_INVALID")
        package_id = item.get("packageId")
        facts = item.get("facts")
        refs = item.get("evidenceRefs")
        rationale = item.get("rationale")
        if (package_id not in expected or package_id in package_ids or item.get("action") not in ACTIONS
                or not isinstance(facts, dict) or not isinstance(facts.get("vendorName"), str)
                or not isinstance(facts.get("invoiceNumber"), str) or type(facts.get("amountMinor")) is not int
                or not isinstance(facts.get("currency"), str) or not isinstance(refs, list) or len(refs) != 3
                or len(set(refs)) != 3
                or not all(isinstance(ref, str) and re.fullmatch(r"\[[^\]]+\]", ref) for ref in refs)
                or not isinstance(rationale, str) or not 60 <= len(rationale) <= 1500
                or sum(ref in rationale for ref in refs) < 2):
            raise AgentError(502, "AI_RESPONSE_INVALID")
        package_text = canonical_json(package_by_id[package_id])
        if any(ref not in package_text for ref in refs):
            raise AgentError(502, "AI_RESPONSE_INVALID")
        action_id = fresh_id()
        while action_id in action_ids:
            action_id = fresh_id()
        package_ids.add(package_id)
        action_ids.add(action_id)
        result.append({"packageId": package_id, "actionId": action_id, "action": item["action"],
                       "facts": {"vendorName": facts["vendorName"], "invoiceNumber": facts["invoiceNumber"],
                                 "amountMinor": facts["amountMinor"], "currency": facts["currency"]},
                       "evidenceRefs": refs, "rationale": rationale})
    return result


def create_app_handler(store, decision_provider, base_url):
    parsed_base = urlparse(base_url)
    base_path = parsed_base.path.rstrip("/")
    if not base_path:
        base_path = "/a2a"
    card = {
        "name": "Invoice Action Agent",
        "description": "Reviews invoice packages and proposes evidence-backed business actions.",
        "version": "1.0.0",
        "capabilities": {"streaming": False, "pushNotifications": False, "stateTransitionHistory": True},
        "supportedInterfaces": [{"url": base_url, "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"}],
        "defaultInputModes": [BATCH_MEDIA, RESULTS_MEDIA],
        "defaultOutputModes": [PROPOSALS_MEDIA, RECEIPTS_MEDIA],
        "skills": [{"id": "invoice_action_agent", "name": "Invoice Action Agent", "description": "Evidence-based invoice decisions with approval-gated execution.", "tags": ["invoice", "payments", "approval"]}],
    }

    class Handler(BaseHTTPRequestHandler):
        server_version = "InvoiceAgent/1.0"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            return

        def send_json(self, status, value, media=A2A_MEDIA):
            body = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            if len(body) > 512 * 1024:
                status, body = 500, b'{"error":{"code":"RESPONSE_TOO_LARGE","message":"Request could not be completed"}}'
            self.send_response(status)
            self.send_header("Content-Type", media)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def fail(self, error):
            self.send_json(error.status, {"error": {"code": error.code, "message": error.message}})

        def authenticate(self):
            auth = self.headers.get("Authorization", "")
            match = re.fullmatch(r"Bearer ([^\s]+)", auth)
            if not match:
                raise AgentError(401, "UNAUTHORIZED")
            return hashlib.sha256(match.group(1).encode("utf-8")).hexdigest()

        def protocol_headers(self, is_post=False):
            if self.headers.get("A2A-Version") != "1.0":
                raise AgentError(400, "A2A_VERSION_UNSUPPORTED")
            if is_post and self.headers.get("Content-Type", "").split(";")[0].strip() != A2A_MEDIA:
                raise AgentError(415, "MEDIA_TYPE_UNSUPPORTED")

        def read_body(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise AgentError(400, "REQUEST_INVALID") from exc
            if length == 0:
                return {}
            if length < 0 or length > 512 * 1024:
                raise AgentError(400, "REQUEST_INVALID")
            try:
                return json.loads(self.rfile.read(length))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise AgentError(400, "REQUEST_INVALID") from exc

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/.well-known/agent-card.json":
                self.send_json(200, card, "application/json")
                return
            try:
                principal = self.authenticate()
                self.protocol_headers()
                if path == f"{base_path}/tasks":
                    self.send_json(200, {"tasks": store.list_tasks(principal)})
                elif path.startswith(f"{base_path}/tasks/"):
                    task_id = path[len(f"{base_path}/tasks/"):]
                    if not task_id or "/" in task_id:
                        raise AgentError(404, "TASK_NOT_FOUND")
                    self.send_json(200, store.get_task(principal, task_id))
                else:
                    raise AgentError(404, "NOT_FOUND")
            except AgentError as exc:
                self.fail(exc)

        def do_POST(self):
            path = urlparse(self.path).path
            try:
                principal = self.authenticate()
                self.protocol_headers(is_post=True)
                body = self.read_body()
                if path == f"{base_path}/message:send":
                    self.send_json(200, {"task": self.handle_message(principal, body)})
                elif path.endswith(":cancel") and path.startswith(f"{base_path}/tasks/"):
                    task_id = path[len(f"{base_path}/tasks/"):-len(":cancel")]
                    self.send_json(200, store.cancel(principal, task_id))
                else:
                    raise AgentError(404, "NOT_FOUND")
            except AgentError as exc:
                self.fail(exc)

        def handle_message(self, principal, body):
            with store.lock:
                return self._handle_message(principal, body)

        def _handle_message(self, principal, body):
            message = body.get("message") if isinstance(body, dict) else None
            if not isinstance(message, dict) or message.get("role") != "ROLE_USER" or not isinstance(message.get("messageId"), str) or not message["messageId"]:
                raise AgentError(400, "MESSAGE_INVALID")
            existing = store.idempotent_task(principal, message["messageId"], digest(message))
            if existing is not None:
                return existing
            parts = message.get("parts")
            if not isinstance(parts, list) or len(parts) != 1 or not isinstance(parts[0], dict):
                raise AgentError(400, "MESSAGE_INVALID")
            media = parts[0].get("mediaType")
            data = parts[0].get("data")
            if media == RESULTS_MEDIA:
                if not isinstance(message.get("taskId"), str) or not isinstance(data, dict):
                    raise AgentError(400, "MESSAGE_INVALID")
                return store.continue_task(principal, message, data)
            if media != BATCH_MEDIA:
                raise AgentError(415, "MEDIA_TYPE_UNSUPPORTED")
            packages = validate_batch(data)
            generated = []
            missing = []
            for package in packages:
                cached = store.cache_get(package)
                if cached is None:
                    missing.append(package)
                else:
                    generated.append(cached)
            if missing:
                generated.extend(decision_provider.decide(data, missing))
                generated = validated_proposals(packages, generated)
                by_id = {proposal["packageId"]: proposal for proposal in generated}
                for package in missing:
                    store.cache_put(package, {key: value for key, value in by_id[package["packageId"]].items() if key != "actionId"})
            else:
                generated = validated_proposals(packages, generated)
            proposals = []
            for proposal in generated:
                proposals.append({"packageId": proposal["packageId"], "actionId": fresh_id(), "action": proposal["action"],
                                  "facts": proposal["facts"], "evidenceRefs": proposal["evidenceRefs"], "rationale": proposal["rationale"]})
            batch_id = data["batchId"]
            task = {"id": fresh_id(), "contextId": message.get("contextId") or fresh_id(), "batchId": batch_id,
                    "status": {"state": "TASK_STATE_INPUT_REQUIRED"},
                    "artifacts": [{"artifactId": fresh_id(), "name": "Invoice action proposals", "parts": [{"mediaType": PROPOSALS_MEDIA, "data": {"batchId": batch_id, "proposals": proposals}}]}],
                    "history": [message]}
            return store.save_initial(principal, message, task)

    return Handler


def main():
    base_url = os.environ.get("A2A_BASE_URL", "http://localhost:8000/a2a/")
    if os.environ.get("A2A_REQUIRE_HTTPS", "0") == "1" and not base_url.startswith("https://"):
        raise SystemExit("A2A_BASE_URL must be public HTTPS when A2A_REQUIRE_HTTPS=1")
    database = os.environ.get("A2A_DATABASE", "./invoice-agent.sqlite3")
    host = os.environ.get("A2A_HOST", "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1")
    port = int(os.environ.get("A2A_PORT", os.environ.get("PORT", "8000")))
    server = ThreadingHTTPServer((host, port), create_app_handler(Store(database), DecisionProvider(), base_url))
    print(f"Invoice agent listening on http://{host}:{port} (base URL: {base_url})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()