"""Unit tests for the drain loop's delivery guarantees (no NATS server)."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from hyperlab_fn_runtime import runtime
from hyperlab_fn_runtime.models import Event
from hyperlab_fn_runtime.runtime import HandlerContext, Settings


class FakeMsg:
    def __init__(self, payload: dict[str, Any], num_delivered: int = 1) -> None:
        self.subject = "comms.test.subject"
        self.data = json.dumps(payload).encode()
        self.metadata = SimpleNamespace(
            num_delivered=num_delivered,
            sequence=SimpleNamespace(stream=7),
        )
        self.acked = False
        self.naked = False
        self.termed = False

    async def ack(self) -> None:
        self.acked = True

    async def nak(self, delay: int = 0) -> None:
        self.naked = True

    async def term(self) -> None:
        self.termed = True


class FakeCtx:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object], str]] = []

    async def publish(
        self, subject: str, payload: dict[str, object], msg_id: str = ""
    ) -> None:
        self.published.append((subject, payload, msg_id))


def _counters() -> tuple[Any, Any, Any]:
    _, ok, failed, dead = runtime._registry()
    return ok, failed, dead


async def _noop(event: Event, ctx: object) -> None:
    return None


async def _boom(event: Event, ctx: object) -> None:
    raise RuntimeError("handler broken")


class TestProcessBatch:
    async def test_success_acks(self) -> None:
        ok, failed, dead = _counters()
        msg = FakeMsg({"id": "x"})

        failures = await runtime._process_batch(
            _noop, [msg], ok, failed, dead, 5.0, FakeCtx()  # type: ignore[arg-type]
        )

        assert failures == 0
        assert msg.acked and not msg.naked and not msg.termed

    async def test_event_carries_stream_seq(self) -> None:
        seen: list[Event] = []

        async def capture(event: Event, ctx: object) -> None:
            seen.append(event)

        ok, failed, dead = _counters()
        await runtime._process_batch(
            capture,
            [FakeMsg({"a": 1})],
            ok,
            failed,
            dead,
            5.0,
            FakeCtx(),  # type: ignore[arg-type]
        )

        assert seen[0].stream_seq == 7

    async def test_failure_naks_and_counts(self) -> None:
        ok, failed, dead = _counters()
        msg = FakeMsg({"id": "x"})

        failures = await runtime._process_batch(
            _boom, [msg], ok, failed, dead, 5.0, FakeCtx()  # type: ignore[arg-type]
        )

        assert failures == 1
        assert msg.naked and not msg.acked and not msg.termed

    async def test_poison_terminated_at_max_deliver(self) -> None:
        ok, failed, dead = _counters()
        msg = FakeMsg({"id": "x"}, num_delivered=runtime.MAX_DELIVER)

        failures = await runtime._process_batch(
            _boom, [msg], ok, failed, dead, 5.0, FakeCtx()  # type: ignore[arg-type]
        )

        assert failures == 1
        assert msg.termed and not msg.naked

    async def test_hanging_handler_times_out_to_nak(self) -> None:
        async def hang(event: Event, ctx: object) -> None:
            await asyncio.sleep(60)

        ok, failed, dead = _counters()
        msg = FakeMsg({"id": "x"})

        failures = await runtime._process_batch(
            hang, [msg], ok, failed, dead, 0.01, FakeCtx()  # type: ignore[arg-type]
        )

        assert failures == 1
        assert msg.naked


class TestSettings:
    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FN_NAME", "fn-x")
        monkeypatch.setenv("FN_SUBJECT", "comms.a.b")
        monkeypatch.setenv("FN_CONSUMER", "fn-x")

        settings = Settings.from_env()
        settings.validate()

        assert settings.stream == "COMMS"
        assert settings.name == "fn-x"

    def test_validate_requires_subject_and_consumer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FN_SUBJECT", raising=False)
        monkeypatch.delenv("FN_CONSUMER", raising=False)

        with pytest.raises(SystemExit):
            Settings.from_env().validate()


class TestConnect:
    async def test_requires_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("NATS_TOKEN", raising=False)

        with pytest.raises(SystemExit):
            await runtime._connect()


class TestHandlerContext:
    async def test_publish_serializes_and_sets_msg_id(self) -> None:
        calls: list[tuple[str, bytes, dict[str, str] | None]] = []

        class FakeJs:
            async def publish(
                self,
                subject: str,
                payload: bytes,
                headers: dict[str, str] | None = None,
            ) -> None:
                calls.append((subject, payload, headers))

        ctx = HandlerContext(FakeJs())  # type: ignore[arg-type]

        await ctx.publish("comms.fn.x.out", {"a": 1}, msg_id="x:1")
        await ctx.publish("comms.fn.x.out", {"b": 2})

        assert calls[0] == (
            "comms.fn.x.out",
            b'{"a":1}',
            {"Nats-Msg-Id": "x:1"},
        )
        assert calls[1][2] is None


class TestEvent:
    def test_json_round_trip(self) -> None:
        event = Event(subject="s", data=b'{"k": 1}', num_delivered=1)

        assert event.json() == {"k": 1}

    def test_non_object_rejected(self) -> None:
        event = Event(subject="s", data=b"[1]", num_delivered=1)

        with pytest.raises(ValueError):
            event.json()
