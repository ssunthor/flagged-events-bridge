# flagged-events-bridge

Minimal Kafka-to-WebSocket bridge for the F1 flagged-events feed (HALO-881,
first slice). Consumes the raw flagged-events topic(s) and re-broadcasts
whatever it receives to any connected WebSocket client, unchanged.

## Why this exists, and why it's separate from `backend/`

GTT's existing dashboard backend (`backend/main.py`) already has a proven
Kafka-consumer-to-WebSocket pattern (`kafka_worker_generic`, used for
`TOPIC_SECURITY`/`TOPIC_NETFLOW`). Per the project convention of keeping F1
additions out of GTT's existing code unless genuinely necessary (the same
principle behind `fingerprint-rules/` being its own service rather than a
new RPC bolted onto `backend/`), this service mirrors that consumer
*pattern* in a small, standalone service instead of adding a new topic
worker into `backend/main.py`.

## Scope: minimal, not the full HALO-881

This does **not** map messages into the frozen contract's `FlaggedEventRef`
shape (`identity`, `kafkaTopic`/`partition`/`offset`, `scoreSpace`,
`anomalyCause` in camelCase). It broadcasts the raw JSON-decodable payload
from Kafka as-is. That mapping is real, separately-scoped work — see
`~/gtt-direct/context/09162026-halo871-flagged-events-topic.md` and the
HALO-881 ticket. This exists to prove a genuine Kafka read reaches the
browser, for a demo, ahead of the full contract-correct feed.

It also does not touch `fingerprint.rules`/`overrides` or
`FingerprintRuleService` at all — that's a separate, Postgres-backed path
(HALO-858), unrelated to this service.

## Configuration

| Env var | Default | Notes |
|---|---|---|
| `KAFKA_FLAGGED_EVENTS_SERVERS` | `10.250.133.163:29092` | Same bootstrap address `backend/main.py`'s other Kafka consumers use by default. |
| `KAFKA_FLAGGED_EVENTS_TOPICS` | `syslog-dfp-output` | Comma-separated. **Placeholder** — confirm the real topic name(s) against the `feature-871-kafka-flagged-events-def` branch before deploying anywhere but a demo. |
| `KAFKA_FLAGGED_EVENTS_GROUP_ID` | `flagged-events-bridge` | Consumer group id. |
| `LOG_LEVEL` | `INFO` | |

## Running locally

```bash
docker build -t flagged-events-bridge .
docker run --rm -p 8000:8000 \
  -e KAFKA_FLAGGED_EVENTS_SERVERS=<broker>:<port> \
  -e KAFKA_FLAGGED_EVENTS_TOPICS=<real-topic-name> \
  flagged-events-bridge
```

Connect a WebSocket client to `ws://localhost:8000/ws`. `/healthz` is a
plain readiness check for the k8s probes.

## Known limitations (demo slice, not the final feature)

- No message shape mapping to `FlaggedEventRef` — raw passthrough only.
- No auth. This is fine for now because it carries the same kind of data
  the existing unauthenticated Kafka listeners already carry (see
  `docs/Insight/Network-Configuration-Requirements.md` §1/§2) and adds no
  new exposure; it should not be treated as a precedent for skipping auth
  on the real HALO-881 service.
- Single pod, in-memory connection set — no fan-out across replicas.
- Topic name is a placeholder pending HALO-871 confirmation.
