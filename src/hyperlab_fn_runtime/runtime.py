"""Shared function runtime: drain a durable JetStream consumer, exit.

Extracted from hyperlab-fn-template (HYP-90) so drain semantics live in
one tested place instead of N generated copies.

ScaledJob semantics: KEDA starts a pod when the durable consumer has
lag (NumPending + NumAckPending); the pod fetches batches until the
consumer is empty, acking per message, then exits 0. ``fn init``
creates the durable consumer (deploy hook) so lag is measurable before
the first message.

Delivery contract: at-least-once up to ``max_deliver`` attempts; a
message failing that many times is terminated (fn_events_dead_total)
so it neither blocks the consumer nor disappears without a metric.

Environment (deploy manifests are the config source):
- FN_NAME (metrics job name, e.g. fn-plaud-digest)
- FN_STREAM / FN_SUBJECT / FN_CONSUMER
- NATS_URL (default nats://nats.nats.svc.cluster.local:4222)
- NATS_TOKEN (required)
- PUSHGATEWAY_URL (optional; metrics skipped when empty)
- FN_HANDLER_TIMEOUT_S (default 60; keep under ack_wait 120)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import sys
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import nats
import nats.errors
import nats.js
import nats.js.errors
from nats.aio.client import Client
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from prometheus_client import CollectorRegistry, Counter, push_to_gateway

from hyperlab_fn_runtime.models import Event

logger = logging.getLogger(__name__)

RUNTIME_VERSION = "2"

BATCH_SIZE = 10
FETCH_TIMEOUT_S = 5
MAX_DELIVER = 5
ACK_WAIT_S = 120
NAK_DELAY_S = 30
# After the consumer looks empty, keep waiting this long for nak-delayed
# redeliveries before exiting; without it a nak'd message on a quiet
# subject waits for the next unrelated event before being retried.
ACK_PENDING_WAIT_BUDGET_S = 90

Handler = Callable[[Event, "HandlerContext"], Awaitable[None]]


@dataclass(frozen=True)
class Settings:
    """Env-derived runtime configuration, read at call time."""

    name: str
    stream: str
    subject: str
    consumer: str

    @classmethod
    def from_env(cls) -> Settings:
        """Read configuration from FN_* environment variables."""
        return cls(
            name=os.environ.get("FN_NAME", "fn-unknown"),
            stream=os.environ.get("FN_STREAM", "COMMS"),
            subject=os.environ.get("FN_SUBJECT", ""),
            consumer=os.environ.get("FN_CONSUMER", ""),
        )

    def validate(self) -> None:
        """Fail fast when the required identifiers are missing."""
        if not self.subject or not self.consumer:
            raise SystemExit("FN_SUBJECT and FN_CONSUMER must be set")


class HandlerContext:
    """Platform services handed to the handler.

    ``publish`` writes through JetStream on the runtime's existing
    connection and awaits the PubAck, so output persistence is
    confirmed before the input message is acked. ``msg_id`` enables
    stream-side dedupe (default window 2 min) for redeliveries.
    """

    def __init__(self, js: nats.js.JetStreamContext) -> None:
        self._js = js

    async def publish(
        self, subject: str, payload: dict[str, object], msg_id: str = ""
    ) -> None:
        """JetStream-acked JSON publish; msg_id enables stream dedupe."""
        headers = {"Nats-Msg-Id": msg_id} if msg_id else None
        await self._js.publish(
            subject,
            json.dumps(payload, separators=(",", ":")).encode(),
            headers=headers,
        )


def _registry() -> tuple[CollectorRegistry, Counter, Counter, Counter]:
    registry = CollectorRegistry()
    ok = Counter("fn_events_processed_total", "acked events", registry=registry)
    failed = Counter("fn_events_failed_total", "nak'd events", registry=registry)
    dead = Counter(
        "fn_events_dead_total",
        "events terminated after MAX_DELIVER failures",
        registry=registry,
    )
    return registry, ok, failed, dead


async def _connect() -> Client:
    token = os.environ.get("NATS_TOKEN", "")
    if not token:
        raise SystemExit("NATS_TOKEN is not set")
    return await nats.connect(
        os.environ.get("NATS_URL", "nats://nats.nats.svc.cluster.local:4222"),
        token=token,
    )


async def init_consumer(settings: Settings) -> None:
    """Create or update the durable pull consumer.

    Idempotent for identical and updatable-field changes (the server
    updates in place). Non-updatable changes (e.g. deliver_policy)
    fail loudly: migrate manually with
    ``nats consumer rm <stream> <consumer>`` and re-run init.
    """
    nc = await _connect()
    try:
        js = nc.jetstream()
        try:
            await js.add_consumer(
                settings.stream,
                ConsumerConfig(
                    durable_name=settings.consumer,
                    filter_subject=settings.subject,
                    ack_policy=AckPolicy.EXPLICIT,
                    deliver_policy=DeliverPolicy.NEW,
                    max_deliver=MAX_DELIVER,
                    ack_wait=ACK_WAIT_S,
                ),
            )
        except nats.js.errors.APIError as exc:
            logger.error(
                "consumer %s on %s cannot be created/updated (%s); if the "
                "config changed a non-updatable field, migrate with "
                "'nats consumer rm %s %s' and re-run init",
                settings.consumer,
                settings.stream,
                exc,
                settings.stream,
                settings.consumer,
            )
            raise
        logger.info(
            "consumer %s ready on %s (%s)",
            settings.consumer,
            settings.stream,
            settings.subject,
        )
    finally:
        await nc.close()


async def _process_batch(
    handler: Handler,
    messages: list[Any],
    ok: Counter,
    failed: Counter,
    dead: Counter,
    handler_timeout: float,
    ctx: HandlerContext,
) -> int:
    failures = 0
    for msg in messages:
        event = Event(
            subject=msg.subject,
            data=msg.data,
            num_delivered=msg.metadata.num_delivered or 1,
            stream_seq=(msg.metadata.sequence.stream if msg.metadata.sequence else 0),
        )
        try:
            await asyncio.wait_for(handler(event, ctx), timeout=handler_timeout)
            await msg.ack()
            ok.inc()
        except Exception:
            failures += 1
            if event.num_delivered >= MAX_DELIVER:
                logger.error(
                    "terminating poison message on %s after %d deliveries "
                    "(stream seq %s)",
                    msg.subject,
                    event.num_delivered,
                    event.stream_seq or "?",
                )
                await msg.term()
                dead.inc()
            else:
                logger.exception("handler failed on %s", msg.subject)
                await msg.nak(delay=NAK_DELAY_S)
                failed.inc()
    return failures


async def _ack_pending(js: nats.js.JetStreamContext, settings: Settings) -> int:
    info = await js.consumer_info(settings.stream, settings.consumer)
    return int(info.num_ack_pending or 0)


async def drain(handler: Handler, settings: Settings) -> int:
    """Process until the consumer is empty; return count of failures."""
    registry, ok, failed, dead = _registry()
    failure_count = 0
    handler_timeout = float(os.environ.get("FN_HANDLER_TIMEOUT_S", "60"))
    nc = await _connect()
    try:
        js = nc.jetstream()
        # Bind only: pull_subscribe without bind would silently CREATE a
        # default consumer (deliver_policy=all, unlimited redelivery)
        # when init has not run, replaying the whole stream.
        sub = await js.pull_subscribe_bind(
            consumer=settings.consumer, stream=settings.stream
        )
        ctx = HandlerContext(js)
        pending_wait_left = float(ACK_PENDING_WAIT_BUDGET_S)
        while True:
            try:
                messages = await sub.fetch(BATCH_SIZE, timeout=FETCH_TIMEOUT_S)
            except asyncio.TimeoutError:
                # covers nats.errors.TimeoutError (subclass) and the
                # bare asyncio.TimeoutError paths inside nats-py fetch
                try:
                    still_pending = await _ack_pending(js, settings)
                except nats.js.errors.APIError:
                    break
                if still_pending > 0 and pending_wait_left > 0:
                    await asyncio.sleep(FETCH_TIMEOUT_S)
                    pending_wait_left -= FETCH_TIMEOUT_S * 2
                    continue
                break
            except (nats.errors.ConnectionClosedError, nats.js.errors.APIError):
                logger.exception("drain aborted: NATS connection/API failure")
                failure_count += 1
                break
            failure_count += await _process_batch(
                handler, messages, ok, failed, dead, handler_timeout, ctx
            )
    finally:
        try:
            await nc.close()
        finally:
            _push_metrics(registry, settings)
    return failure_count


def _push_metrics(registry: CollectorRegistry, settings: Settings) -> None:
    url = os.environ.get("PUSHGATEWAY_URL", "")
    if not url:
        return
    try:
        push_to_gateway(
            url,
            job=settings.name,
            grouping_key={"instance": socket.gethostname()},
            registry=registry,
        )
    except Exception:
        logger.warning("pushgateway unreachable; metrics dropped")


def main(handler: Handler) -> None:
    """CLI entry for generated functions: ``fn drain`` / ``fn init``."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    settings = Settings.from_env()
    settings.validate()
    command = sys.argv[1] if len(sys.argv) > 1 else "drain"
    if command == "init":
        asyncio.run(init_consumer(settings))
    elif command == "drain":
        failures = asyncio.run(drain(handler, settings))
        # Exit 0 even with failures: nak'd messages redeliver and KEDA
        # rescales (lag counts NumAckPending); a non-zero exit would
        # make the Job retry the whole drain immediately and mask the
        # per-message failure metrics.
        logger.info("drain complete: %d failures", failures)
    else:
        raise SystemExit(f"unknown command: {command}")
