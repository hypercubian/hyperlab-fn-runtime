"""Shared function runtime: drain a durable JetStream consumer, exit.

Extracted from hyperlab-fn-template (HYP-90) so drain semantics live in
one tested place instead of N generated copies.

ScaledJob semantics: KEDA starts a pod when the durable consumer has
lag (NumPending + NumAckPending); the pod fetches batches until the
consumer is empty, acking per message, then exits 0. ``fn init``
creates the durable consumer (deploy hook) so lag is measurable before
the first message.

Delivery contract: at-least-once up to the consumer's ``max_deliver``
attempts; a message failing that many times (or raising
:class:`PermanentError`) is terminated (fn_events_dead_total) so it
neither blocks the consumer nor disappears without a metric. The
``drain`` command ALWAYS exits 0: nak'd messages redeliver and KEDA
rescales on lag; a non-zero exit would make the Job retry the whole
drain immediately and mask per-message failure metrics.

Environment (deploy manifests are the config source): FN_NAME,
FN_STREAM, FN_SUBJECT, FN_CONSUMER, NATS_URL, NATS_TOKEN (required),
PUSHGATEWAY_URL, FN_BATCH_SIZE, FN_MAX_DELIVER, FN_ACK_WAIT_S,
FN_NAK_DELAY_S, FN_HANDLER_TIMEOUT_S, LOG_LEVEL.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
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

# Internal loop tuning; not per-function policy.
FETCH_TIMEOUT_S = 5
ACK_PENDING_WAIT_BUDGET_S = 90


class ConfigError(Exception):
    """Invalid runtime configuration; ``main`` converts to SystemExit."""


class PermanentError(Exception):
    """Handler failure that must not be retried; terminate immediately."""


@dataclass(frozen=True)
class Settings:
    """Env-derived runtime configuration, read at call time.

    ``NATS_TOKEN`` deliberately stays out: a frozen dataclass gets a
    free ``__repr__`` and Settings is the object most likely to be
    logged.
    """

    name: str
    stream: str
    subject: str
    consumer: str
    nats_url: str = "nats://nats.nats.svc.cluster.local:4222"
    pushgateway_url: str = ""
    batch_size: int = 10
    max_deliver: int = 5
    ack_wait_s: int = 120
    nak_delay_s: int = 30
    handler_timeout_s: float = 60.0

    @classmethod
    def from_env(cls) -> Settings:
        """Read configuration from FN_* environment variables."""
        env = os.environ
        consumer = env.get("FN_CONSUMER", "")
        name = env.get("FN_NAME", "")
        if not name and consumer:
            logger.warning("FN_NAME unset; using consumer name %r", consumer)
            name = consumer
        try:
            return cls(
                name=name,
                stream=env.get("FN_STREAM", "COMMS"),
                subject=env.get("FN_SUBJECT", ""),
                consumer=consumer,
                nats_url=env.get("NATS_URL", "nats://nats.nats.svc.cluster.local:4222"),
                pushgateway_url=env.get("PUSHGATEWAY_URL", ""),
                batch_size=int(env.get("FN_BATCH_SIZE", "10")),
                max_deliver=int(env.get("FN_MAX_DELIVER", "5")),
                ack_wait_s=int(env.get("FN_ACK_WAIT_S", "120")),
                nak_delay_s=int(env.get("FN_NAK_DELAY_S", "30")),
                handler_timeout_s=float(env.get("FN_HANDLER_TIMEOUT_S", "60")),
            )
        except ValueError as exc:
            raise ConfigError(f"non-numeric FN_* value: {exc}") from exc

    def validate(self) -> None:
        """Fail fast when configuration cannot possibly work."""
        for field_name in ("name", "stream", "subject", "consumer"):
            if not getattr(self, field_name):
                raise ConfigError(f"{field_name} must be set (FN_* env)")
        if self.batch_size < 1 or self.max_deliver < 1:
            raise ConfigError("FN_BATCH_SIZE and FN_MAX_DELIVER must be >= 1")
        if self.handler_timeout_s >= self.ack_wait_s:
            raise ConfigError(
                f"FN_HANDLER_TIMEOUT_S={self.handler_timeout_s} must stay "
                f"under ack_wait={self.ack_wait_s}s or redelivery races the "
                "handler (duplicate processing)"
            )


class HandlerContext:
    """Platform services handed to the handler.

    ``publish`` writes through JetStream on the runtime's existing
    connection and awaits the PubAck, so output persistence is
    confirmed before the input message is acked. ``msg_id`` enables
    stream-side dedupe (default window 2 min) for redeliveries.
    """

    def __init__(
        self,
        js: nats.js.JetStreamContext,
        settings: Settings,
        registry: CollectorRegistry,
    ) -> None:
        self._js = js
        self.settings = settings
        self.registry = registry

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

    def counter(self, name: str, documentation: str) -> Counter:
        """Register a function-specific counter on the pushed registry."""
        return Counter(name, documentation, registry=self.registry)


Handler = Callable[[Event, HandlerContext], Awaitable[None]]


def _registry() -> tuple[CollectorRegistry, Counter, Counter, Counter]:
    registry = CollectorRegistry()
    ok = Counter("fn_events_processed_total", "acked events", registry=registry)
    failed = Counter("fn_events_failed_total", "nak'd events", registry=registry)
    dead = Counter(
        "fn_events_dead_total",
        "events terminated after max_deliver failures or PermanentError",
        registry=registry,
    )
    return registry, ok, failed, dead


async def _connect(settings: Settings) -> Client:
    token = os.environ.get("NATS_TOKEN", "")
    if not token:
        raise ConfigError("NATS_TOKEN is not set")
    return await nats.connect(settings.nats_url, token=token)


async def init_consumer(settings: Settings) -> None:
    """Create or update the durable pull consumer.

    Idempotent for identical and updatable-field changes (the server
    updates in place). Non-updatable changes (e.g. deliver_policy)
    fail loudly: migrate manually with
    ``nats consumer rm <stream> <consumer>`` and re-run init.
    """
    nc = await _connect(settings)
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
                    max_deliver=settings.max_deliver,
                    ack_wait=settings.ack_wait_s,
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


async def _settle(msg: Any, action: str, delay: int = 0) -> None:
    """Ack/nak/term without letting a NATS blip abort the whole drain."""
    try:
        if action == "ack":
            await msg.ack()
        elif action == "term":
            await msg.term()
        else:
            await msg.nak(delay=delay)
    except Exception:
        logger.warning("could not %s message on %s", action, msg.subject)


async def _process_batch(
    handler: Handler,
    messages: list[Any],
    ok: Counter,
    failed: Counter,
    dead: Counter,
    settings: Settings,
    max_deliver: int,
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
            await asyncio.wait_for(
                handler(event, ctx), timeout=settings.handler_timeout_s
            )
            await _settle(msg, "ack")
            ok.inc()
        except PermanentError:
            failures += 1
            logger.error(
                "permanent failure on %s (stream seq %s); terminating",
                msg.subject,
                event.stream_seq or "?",
            )
            await _settle(msg, "term")
            dead.inc()
        except Exception:
            failures += 1
            if event.num_delivered >= max_deliver:
                logger.error(
                    "terminating poison message on %s after %d deliveries "
                    "(stream seq %s)",
                    msg.subject,
                    event.num_delivered,
                    event.stream_seq or "?",
                )
                await _settle(msg, "term")
                dead.inc()
            else:
                logger.exception("handler failed on %s", msg.subject)
                await _settle(msg, "nak", delay=settings.nak_delay_s)
                failed.inc()
    return failures


async def _consumer_limits(js: nats.js.JetStreamContext, settings: Settings) -> int:
    """Server-side max_deliver: the consumer config wins over env.

    Prevents split-brain when the library or env changes but the
    durable consumer keeps its original configuration.
    """
    info = await js.consumer_info(settings.stream, settings.consumer)
    server_max = int(info.config.max_deliver or settings.max_deliver)
    if server_max != settings.max_deliver:
        logger.warning(
            "consumer max_deliver=%d differs from configured %d; " "server value wins",
            server_max,
            settings.max_deliver,
        )
    return server_max


async def _ack_pending(js: nats.js.JetStreamContext, settings: Settings) -> int:
    info = await js.consumer_info(settings.stream, settings.consumer)
    return int(info.num_ack_pending or 0)


async def drain(handler: Handler, settings: Settings) -> int:
    """Process until the consumer is empty; return count of failures."""
    registry, ok, failed, dead = _registry()
    failure_count = 0
    nc = await _connect(settings)
    try:
        js = nc.jetstream()
        # Bind only: pull_subscribe without bind would silently CREATE a
        # default consumer (deliver_policy=all, unlimited redelivery)
        # when init has not run, replaying the whole stream.
        sub = await js.pull_subscribe_bind(
            consumer=settings.consumer, stream=settings.stream
        )
        max_deliver = await _consumer_limits(js, settings)
        ctx = HandlerContext(js, settings, registry)
        loop = asyncio.get_running_loop()
        # Wall-clock deadline for waiting on nak-delayed redeliveries
        # once the consumer looks empty; RESET on every successful
        # fetch so earlier idle gaps do not consume the budget.
        deadline: float | None = None
        while True:
            try:
                messages = await sub.fetch(settings.batch_size, timeout=FETCH_TIMEOUT_S)
            except asyncio.TimeoutError:
                # also covers nats.errors.TimeoutError (subclass)
                try:
                    still_pending = await _ack_pending(js, settings)
                except (nats.errors.Error, nats.js.errors.APIError):
                    break
                if still_pending <= 0:
                    break
                now = loop.time()
                if deadline is None:
                    deadline = now + ACK_PENDING_WAIT_BUDGET_S
                if now >= deadline:
                    break
                await asyncio.sleep(FETCH_TIMEOUT_S)
                continue
            except (nats.errors.ConnectionClosedError, nats.js.errors.APIError):
                logger.exception("drain aborted: NATS connection/API failure")
                failure_count += 1
                break
            deadline = None  # progress: restart the redelivery budget
            failure_count += await _process_batch(
                handler, messages, ok, failed, dead, settings, max_deliver, ctx
            )
    finally:
        try:
            await nc.close()
        finally:
            _push_metrics(registry, settings)
    return failure_count


def _push_metrics(registry: CollectorRegistry, settings: Settings) -> None:
    if not settings.pushgateway_url:
        return
    try:
        # Stable grouping key: per-pod hostnames would accumulate one
        # immortal Pushgateway group per drain run.
        push_to_gateway(
            settings.pushgateway_url,
            job=settings.name,
            grouping_key={"instance": settings.name},
            registry=registry,
        )
    except Exception:
        logger.warning("pushgateway unreachable; metrics dropped")


def main(handler: Handler, argv: list[str] | None = None) -> None:
    """CLI entry for generated functions: ``fn drain`` / ``fn init``."""
    args = sys.argv[1:] if argv is None else argv
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    command = args[0] if args else "drain"
    if command in {"-h", "--help"}:
        print("usage: fn [drain|init]")
        return
    try:
        settings = Settings.from_env()
        settings.validate()
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc
    if command == "init":
        try:
            asyncio.run(init_consumer(settings))
        except ConfigError as exc:
            raise SystemExit(str(exc)) from exc
    elif command == "drain":
        try:
            failures = asyncio.run(drain(handler, settings))
        except ConfigError as exc:
            raise SystemExit(str(exc)) from exc
        except Exception:
            # The drain exit-0 contract is absolute: infrastructure
            # failures redeliver via ack_wait and KEDA rescales on lag.
            logger.exception("drain aborted; exiting 0 so KEDA rescales")
            failures = 0
        logger.info("drain complete: %d failures", failures)
    else:
        raise SystemExit(f"unknown command: {command}")
