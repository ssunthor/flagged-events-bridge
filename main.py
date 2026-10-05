"""flagged-events-bridge (HALO-881, minimal slice).

A small, standalone Kafka-to-WebSocket bridge for the F1 flagged-events
feed. Deliberately net new: this mirrors the Kafka-consumer pattern already
proven in backend/main.py's kafka_worker_generic() (poll loop run in a
thread executor, one Consumer per topic set), but is not built by importing
or editing that file. GTT's existing dashboard backend is left untouched;
this service is additive, matching the same isolation principle applied to
fingerprint-rules (HALO-858).

Scope, on purpose: this is the minimal real-data slice for a demo, not the
full HALO-881. It does NOT map messages into the frozen contract's
FlaggedEventRef shape (identity/kafka coordinates/score_space/anomalyCause
in camelCase) -- that mapping is real, unscoped work, tracked in
~/gtt-direct/context/09162026-halo871-flagged-events-topic.md and the
HALO-881 ticket itself. This service broadcasts whatever JSON-decodable
payload arrives on the configured topic(s), unchanged, so the frontend can
prove a genuine Kafka read is happening. Replacing this with a
contract-shaped feed is follow-on work, not a rewrite of this file's
structure.

Also does not touch fingerprint.rules/overrides or FingerprintRuleService --
that path (Postgres-backed, HALO-858) is a completely separate concern.
This service only reads Kafka and writes to its own WebSocket clients.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from confluent_kafka import Consumer, KafkaException
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("flagged-events-bridge")

# Same env-var shape as backend/main.py's KAFKA_CONF_SECURITY / TOPIC_SECURITY
# (bootstrap.servers + topic name from the environment, no hardcoded cluster
# address baked into code) -- mirrored for operational consistency, not
# imported. KAFKA_FLAGGED_EVENTS_TOPICS is a comma-separated list because the
# real feed spans three pipelines (syslog/netflow/fortinet); default here is
# a placeholder until the HALO-871 topic names are confirmed and wired in.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_FLAGGED_EVENTS_SERVERS", "10.250.133.163:29092")
KAFKA_TOPICS = [
    t.strip()
    for t in os.getenv("KAFKA_FLAGGED_EVENTS_TOPICS", "syslog-dfp-output").split(",")
    if t.strip()
]
KAFKA_GROUP_ID = os.getenv("KAFKA_FLAGGED_EVENTS_GROUP_ID", "flagged-events-bridge")

thread_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-poll")


class ConnectionManager:
    """Tracks connected WebSocket clients and fans out broadcasts to all of
    them. Deliberately simple -- single-process, in-memory. No fan-out
    across replicas is needed for a demo-scoped service with one pod."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._connections.add(ws)
        logger.info("client connected, %d active", len(self._connections))

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(ws)
        logger.info("client disconnected, %d active", len(self._connections))

    async def broadcast(self, payload: dict[str, Any]) -> None:
        message = json.dumps(payload)
        async with self._lock:
            targets = list(self._connections)
        for ws in targets:
            try:
                await ws.send_text(message)
            except Exception:  # noqa: BLE001 -- a dead socket must not stop the broadcast loop
                async with self._lock:
                    self._connections.discard(ws)


manager = ConnectionManager()
app = FastAPI(title="flagged-events-bridge")


def _poll_batch(consumer: Consumer, timeout: float = 1.0, max_messages: int = 50) -> list[Any]:
    """Blocking poll, run inside the thread pool -- same reason
    kafka_worker_generic() in backend/main.py does this: confluent_kafka's
    poll() is synchronous and must not block the asyncio event loop that
    also serves WebSocket connections."""
    messages = []
    for _ in range(max_messages):
        msg = consumer.poll(timeout=timeout)
        if msg is None:
            break
        if msg.error():
            logger.warning("Kafka error: %s", msg.error())
            continue
        messages.append(msg)
    return messages


def _decode(msg) -> dict[str, Any]:
    """Best-effort decode. The real message schema depends on which
    pipeline produced it and isn't finalized (see the module docstring) --
    this never raises on an unparseable payload, it degrades to passing the
    raw bytes through as a string so the demo still shows *something* real
    rather than dropping the message silently."""
    raw = msg.value()
    try:
        value: Any = json.loads(raw)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
        value = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else raw
    return {
        "type": "flagged_event",
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "value": value,
    }


async def kafka_consumer_loop() -> None:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "latest",
    }
    try:
        consumer = Consumer(conf)
        consumer.subscribe(KAFKA_TOPICS)
        logger.info("subscribed to %s via %s", KAFKA_TOPICS, KAFKA_BOOTSTRAP_SERVERS)
    except KafkaException as exc:
        logger.error("failed to start Kafka consumer: %s", exc)
        return

    loop = asyncio.get_running_loop()
    try:
        while True:
            messages = await loop.run_in_executor(thread_pool, _poll_batch, consumer)
            for msg in messages:
                await manager.broadcast(_decode(msg))
            if not messages:
                await asyncio.sleep(0.2)
    finally:
        consumer.close()


@app.on_event("startup")
async def on_startup() -> None:
    asyncio.create_task(kafka_consumer_loop())


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:
            # This bridge is broadcast-only; any inbound message is drained
            # and ignored so the connection doesn't back up.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(websocket)
