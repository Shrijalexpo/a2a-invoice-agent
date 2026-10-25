import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from invoice_agent import (
    A2A_MEDIA,
    BATCH_MEDIA,
    PROPOSALS_MEDIA,
    RECEIPTS_MEDIA,
    RESULTS_MEDIA,
    AgentError,
    DecisionProvider,
    Store,
    create_app_handler,
    validated_proposals,
)


class StubProvider:
    def __init__(self):
        self.calls = 0

    def decide(self, batch, packages):
        self.calls += 1
        return [{
            "packageId": package["packageId"],
            "action": "settle_invoice",
            "facts": {"vendorName": "Acme", "invoiceNumber": "INV-100", "amountMinor": 1000, "currency": "INR"},
            "evidenceRefs": ["[DOC-1]", "[DOC-2]", "[DOC-3]"],
            "rationale": "settle_invoice is supported by [DOC-1] and [DOC-2], with [DOC-3] confirming the invoice details.",
        } for package in packages]


class InvoiceAgentTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tempdir.name) / "agent.sqlite3")
        self.provider = StubProvider()
        handler = create_app_handler(self.store, self.provider, "https://example.test/a2a/")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.root = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tempdir.cleanup()

    def request(self, method, path, body=None, token="user-a", headers=None):
        request_headers = {"A2A-Version": "1.0", "Authorization": f"Bearer {token}"}
        if body is not None:
            request_headers["Content-Type"] = A2A_MEDIA
        request_headers.update(headers or {})
        request = urllib.request.Request(self.root + path, data=json.dumps(body).encode() if body is not None else None,
                                         headers=request_headers, method=method)
        try:
            response = urllib.request.urlopen(request)
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())
        return response.status, json.loads(response.read())

    def initial(self, message_id="msg-1", package_id="pkg-1"):
        return {"message": {"messageId": message_id, "role": "ROLE_USER", "parts": [{"mediaType": BATCH_MEDIA,
                "data": {"batchId": "batch-1", "policyRevision": "r1", "packages": [{"packageId": package_id,
                    "documents": [{"text": "[DOC-1] [DOC-2] [DOC-3]"}]}]}}]},
                "configuration": {"returnImmediately": False}}

    def test_card_is_public_and_has_required_interfaces(self):
        response = urllib.request.urlopen(self.root + "/.well-known/agent-card.json")
        card = json.loads(response.read())
        self.assertEqual(card["supportedInterfaces"][0]["url"], "https://example.test/a2a/")
        self.assertIn(BATCH_MEDIA, card["defaultInputModes"])
        self.assertIn(PROPOSALS_MEDIA, card["defaultOutputModes"])

    def test_initial_task_replay_and_semantic_conflict(self):
        status, response = self.request("POST", "/a2a/message:send", self.initial())
        self.assertEqual(status, 200)
        task = response["task"]
        self.assertEqual(task["status"]["state"], "TASK_STATE_INPUT_REQUIRED")
        self.assertEqual(task["history"][0]["messageId"], "msg-1")
        self.assertTrue(task["artifacts"][0]["artifactId"])
        self.assertEqual(task["artifacts"][0]["parts"][0]["mediaType"], PROPOSALS_MEDIA)
        self.assertEqual(len(task["artifacts"][0]["parts"][0]["data"]["proposals"]), 1)
        self.assertEqual(self.provider.calls, 1)
        status, replay = self.request("POST", "/a2a/message:send", self.initial())
        self.assertEqual(status, 200)
        self.assertEqual(replay["task"], task)
        self.assertEqual(self.provider.calls, 1)
        changed = self.initial()
        changed["message"]["parts"][0]["data"]["batchId"] = "changed"
        self.assertEqual(self.request("POST", "/a2a/message:send", changed)[0], 409)

    def test_concurrent_equivalent_requests_make_one_model_call(self):
        barrier = threading.Barrier(2)
        responses = []
        def submit():
            barrier.wait()
            responses.append(self.request("POST", "/a2a/message:send", self.initial()))
        threads = [threading.Thread(target=submit), threading.Thread(target=submit)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual([status for status, _ in responses], [200, 200])
        self.assertEqual(responses[0][1]["task"], responses[1][1]["task"])
        self.assertEqual(self.provider.calls, 1)

    def test_semantic_package_cache_survives_new_package_id(self):
        self.assertEqual(self.request("POST", "/a2a/message:send", self.initial())[0], 200)
        next_batch = self.initial("msg-2", "pkg-2")
        next_batch["message"]["parts"][0]["data"]["batchId"] = "batch-2"
        status, response = self.request("POST", "/a2a/message:send", next_batch)
        self.assertEqual(status, 200)
        proposal = response["task"]["artifacts"][0]["parts"][0]["data"]["proposals"][0]
        self.assertEqual(proposal["packageId"], "pkg-2")
        self.assertEqual(proposal["facts"]["invoiceNumber"], "INV-100")
        self.assertEqual(self.provider.calls, 1)

    def test_accepted_result_adds_receipt_and_replay_is_stable(self):
        _, response = self.request("POST", "/a2a/message:send", self.initial())
        task = response["task"]
        proposal = task["artifacts"][0]["parts"][0]["data"]["proposals"][0]
        continuation = {"message": {"messageId": "result-1", "taskId": task["id"], "contextId": task["contextId"],
            "role": "ROLE_USER", "parts": [{"mediaType": RESULTS_MEDIA, "data": {"batchId": "batch-1", "results": [{
                "packageId": proposal["packageId"], "actionId": proposal["actionId"], "action": proposal["action"],
                "outcome": "ACCEPTED", "receiptNonce": "nonce-from-grader"}]}}]}}
        status, result = self.request("POST", "/a2a/message:send", continuation)
        self.assertEqual(status, 200)
        self.assertEqual(result["task"]["status"]["state"], "TASK_STATE_COMPLETED")
        receipts = result["task"]["artifacts"][1]["parts"][0]
        self.assertTrue(result["task"]["artifacts"][1]["artifactId"])
        self.assertEqual(receipts["mediaType"], RECEIPTS_MEDIA)
        self.assertEqual(receipts["data"]["executions"][0]["receiptNonce"], "nonce-from-grader")
        self.assertEqual(self.request("POST", "/a2a/message:send", continuation)[1]["task"], result["task"])

    def test_cross_user_task_access_is_hidden_and_list_is_isolated(self):
        _, response = self.request("POST", "/a2a/message:send", self.initial())
        task_id = response["task"]["id"]
        self.assertEqual(self.request("GET", f"/a2a/tasks/{task_id}", token="user-b")[0], 404)
        self.assertEqual(self.request("GET", "/a2a/tasks", token="user-b")[1], {"tasks": []})

    def test_cancel_and_result_have_one_terminal_winner(self):
        _, response = self.request("POST", "/a2a/message:send", self.initial())
        task = response["task"]
        proposal = task["artifacts"][0]["parts"][0]["data"]["proposals"][0]
        continuation = {"message": {"messageId": "race-result", "taskId": task["id"], "contextId": task["contextId"],
            "role": "ROLE_USER", "parts": [{"mediaType": RESULTS_MEDIA, "data": {"batchId": "batch-1", "results": [{
                "packageId": proposal["packageId"], "actionId": proposal["actionId"], "action": proposal["action"],
                "outcome": "ACCEPTED", "receiptNonce": "nonce"}]}}]}}
        barrier = threading.Barrier(2)
        outcomes = []
        def run_result():
            barrier.wait()
            outcomes.append(self.request("POST", "/a2a/message:send", continuation)[0])
        def run_cancel():
            barrier.wait()
            outcomes.append(self.request("POST", f"/a2a/tasks/{task['id']}:cancel", {})[0])
        threads = [threading.Thread(target=run_result), threading.Thread(target=run_cancel)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(outcomes), [200, 409])
        status, final_task = self.request("GET", f"/a2a/tasks/{task['id']}")
        self.assertEqual(status, 200)
        state = final_task["status"]["state"]
        self.assertIn(state, ("TASK_STATE_CANCELED", "TASK_STATE_COMPLETED"))
        self.assertEqual(len(final_task["artifacts"]), 1 if state == "TASK_STATE_CANCELED" else 2)

    def test_proposal_validation_rejects_missing_decisive_references(self):
        bad = self.provider.decide({}, [{"packageId": "pkg"}])
        bad[0]["evidenceRefs"] = ["cover-sheet"]
        with self.assertRaises(AgentError):
            validated_proposals([{"packageId": "pkg"}], bad)

    def test_ai_pipe_responses_request_and_output_parsing(self):
        http_response = Mock()
        http_response.read.return_value = json.dumps({"output": [{"type": "message", "content": [{
            "type": "output_text", "text": json.dumps({"proposals": []})
        }]}]}).encode()
        opener = MagicMock()
        opener.return_value.__enter__.return_value = http_response
        with patch.dict(os.environ, {"AIPIPE_TOKEN": "test-token"}, clear=True), \
                patch("invoice_agent.urllib.request.urlopen", opener):
            result = DecisionProvider().decide({"batchId": "batch-x", "policyRevision": "r1"}, [])
        self.assertEqual(result, [])
        request = opener.call_args.args[0]
        self.assertEqual(request.full_url, "https://aipipe.org/openrouter/v1/responses")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
        body = json.loads(request.data)
        self.assertEqual(body["model"], "openai/gpt-4.1-nano")
        self.assertIn("batch-x", body["input"])
        self.assertEqual(body["text"]["format"]["type"], "json_object")


if __name__ == "__main__":
    unittest.main()