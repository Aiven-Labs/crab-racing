# Deploying to Aiven App Platform

Notes on deploying a new App Platform instance — what the CLI can and can't do, and what actually worked.

## Prerequisites

These Aiven services must exist before deploying:

| Service | Type | Purpose |
|---|---|---|
| `crab-race-kafka` | Kafka | Race move event stream |
| `crab-race-valkey` | Valkey | Ephemeral leaderboard |

## CLI Attempts

### Attempt 1 — VPC conflict

```
avn service create crab-racing-2 \
  --project jay-miller \
  --service-type application \
  --plan startup-50-1024 \
  --cloud aws-eu-west-1 \
  -c application.source.branch=main \
  -c application.source.repository_url=https://github.com/... \
  -c 'application.ports=[...]' \
  -c 'application.environment_variables=[...]'
```

**Error:** `Cloud aws-eu-west-1 has a VPC. Specify --project-vpc-id=<vpc-id> or --no-project-vpc`

**Fix:** add `--no-project-vpc`

### Attempt 2 — CLI crash on array config

Adding `--no-project-vpc` resolved the VPC error. The CLI then crashed:

```
KeyError: 'type'
  File "aiven/client/cli.py", line 212, in collect_user_config_options
    types = spec["type"]
```

This is a bug in `aiven-client` — it cannot handle array-type `-c` fields (`environment_variables`, `ports`). The CLI is unusable for App Platform service creation.

## Working Approach — REST API

```bash
POST https://api.aiven.io/v1/project/{project}/service
Authorization: Bearer {AIVEN_TOKEN}
Content-Type: application/json
```

Payload:

```json
{
  "cloud": "aws-eu-west-1",
  "plan": "startup-50-1024",
  "service_name": "your-service-name",
  "service_type": "application",
  "user_config": {
    "application": {
      "environment_variables": [
        {"key": "PORT", "kind": "variable", "value": "3000"},
        {"key": "KAFKA_BROKER", "kind": "variable", "value": "<redacted>"},
        {"key": "KAFKA_SECURITY_PROTOCOL", "kind": "variable", "value": "SSL"},
        {"key": "KAFKA_CA_CERT", "kind": "variable", "value": "<pem inline>"},
        {"key": "KAFKA_ACCESS_CERT", "kind": "variable", "value": "<pem inline>"},
        {"key": "KAFKA_ACCESS_KEY", "kind": "variable", "value": "<pem inline>"},
        {"key": "VALKEY_URI", "kind": "variable", "value": "<redacted>"},
        {"key": "VALKEY_CA_CERT", "kind": "variable", "value": "<pem inline>"},
        {"key": "PUBLIC_WS_URL", "kind": "variable", "value": "<set after deploy>"}
      ],
      "ports": [{"name": "Port 1", "port": 3000, "protocol": "HTTP"}],
      "source": {
        "branch": "main",
        "build_path": "./",
        "containerfile_path": "./Dockerfile",
        "remote_repository_id": "<github repo id>",
        "repository_url": "https://github.com/your-org/your-repo.git",
        "vcs_integration_id": "<copy from existing service>"
      }
    },
    "ip_filter": ["0.0.0.0/0", "::/0"]
  }
}
```

The `remote_repository_id` and `vcs_integration_id` can be copied from an existing service:

```bash
avn service get <name> --project <project> --json \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['user_config']['application']['source'])"
```

## Getting the Public URL

The `service_uri` in the create response is incomplete. The real URL is in `components` once the service is `RUNNING`:

```bash
avn service get <name> --project <project> --json \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['state'], d['components'][0]['host'])"
```

WebSocket URL: `wss://` + that hostname (no port, no path).

## Required Environment Variables

| Key | Source | Notes |
|---|---|---|
| `PORT` | static | Always `3000` |
| `KAFKA_BROKER` | `avn service get` | `service_uri_params.host:port` |
| `KAFKA_SECURITY_PROTOCOL` | static | Always `SSL` for Aiven |
| `KAFKA_CA_CERT` | `avn service user-creds-download` | PEM inline, `\n` escaped |
| `KAFKA_ACCESS_CERT` | `avn service user-creds-download` | PEM inline, `\n` escaped |
| `KAFKA_ACCESS_KEY` | `avn service user-creds-download` | PEM inline, `\n` escaped |
| `VALKEY_URI` | `avn service get` | `service_uri` field |
| `VALKEY_CA_CERT` | `avn project ca-get` | Project CA; shared across all services |
| `PUBLIC_WS_URL` | post-deploy | Set after service reaches RUNNING |

PEM values must be stored as a single line with literal `\n` escape sequences.

## Credential Script

`scripts/update-env.sh` fetches fresh credentials from Aiven and stores them encrypted in `fnox.toml`. Re-run after any key rotation.

**fnox gotcha:** passing `--description` to `fnox set` alongside piped stdin silently drops the value. Omit `--description` when setting values via stdin.
