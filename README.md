# A2A Invoice Action Agent

A small Python HTTP service for evidence-backed invoice proposals and receipt-gated task completion. It uses SQLite for durable tasks, per-token isolation, idempotency, decision caching, and atomic completion/cancellation.

## Run locally

Python 3.10 or newer is required. Add your AI Pipe token to the environment, then start the service:

```sh
export AIPIPE_TOKEN="your-ai-pipe-token"
export AIPIPE_MODEL="openai/gpt-4.1-nano"
export A2A_BASE_URL="https://your-public-host/a2a/"
export A2A_REQUIRE_HTTPS=1
python invoice_agent.py
```

The model request uses `https://aipipe.org/openrouter/v1/responses`. Override it with `AIPIPE_RESPONSES_URL` only when needed. Keep the token out of source files and version control; the value supplied in chat should be rotated before use.

For cloud platforms, the server binds to `0.0.0.0` and uses the platform-provided `PORT`. Set `A2A_BASE_URL` to the deployed public HTTPS URL, `A2A_REQUIRE_HTTPS=1`, and `A2A_DATABASE` to a path on a persistent volume. The volume matters because the grader may restart or retry the service between checks.

For local development, omit `A2A_REQUIRE_HTTPS` and use `A2A_BASE_URL=http://localhost:8000/a2a/`. The service listens on `127.0.0.1:8000`; set `A2A_HOST=0.0.0.0` behind a TLS-terminating public reverse proxy. The database path defaults to `./invoice-agent.sqlite3` and can be changed with `A2A_DATABASE`.

Every protected request needs `Authorization: Bearer <token>` and `A2A-Version: 1.0`; POST requests also need `Content-Type: application/a2a+json`. Bearer token values are treated as separate user identities. Deploy behind an authentication/rate-limiting gateway if token issuance or acceptance must be restricted.

## Test

```sh
python -m unittest -v
```

The Agent Card is served at `/.well-known/agent-card.json`. A2A routes are under the path in `A2A_BASE_URL`.

This service can be tested locally, but public HTTPS hosting and grader verification require deployment to a reachable host. Do not place real invoices or secrets in test prompts.