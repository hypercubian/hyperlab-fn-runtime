"""Unit tests for the drain loop's delivery guarantees (no NATS server)."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from hyperlab_fn_runtime import runtime
from hyperlab_fn_runtime.models import Event
from hyperlab_fn_runtime.runtime import (
    ConfigError,
    HandlerContext,
    PermanentError,
    Settings,
)

SETTINGS = Settings(
    name="fn-x",
    stream="COMMS",
    subject="comms.test.subject",
    consumer="fn-x",
    handler_timeout_s=5.0,
)


class FakeMsg:
    def __init__(
        self,
        payload: dict[str, Any],
        num_delivered: int = 1,
        fail_settle: bool = False,
    ) -> None:
        self.subject = "comms.test.subject"
        self.data = json.dumps(payload).encode()
        self.metadata = SimpleNamespace(
            num_delivered=num_delivered,
            sequence=SimpleNamespace(stream=7),
        )
        self.fail_settle = fail_settle
        self.acked = False
        self.naked = False
        self.termed = False

    async def ack(self) -> None:
        if self.fail_settle:
            raise RuntimeError("connection lost")
        self.acked = True

    async def nak(self, delay: int = 0) -> None:
        if self.fail_settle:
            raise RuntimeError("connection lost")
        self.naked = True

    async def term(self) -> None:
        self.termed = True


def _ctx() -> HandlerContext:
    js: Any = object()
    return HandlerContext(js, SETTINGS, CollectorRegistry())


def _counters() -> tuple[Any, Any, Any]:
    _, ok, failed, dead = runtime._registry()
    return ok, failed, dead


async def _noop(event: Event, ctx: object) -> None:
    return None


async def _boom(event: Event, ctx: object) -> None:
    raise RuntimeError("handler broken")


async def _batch(handler: Any, msgs: list[FakeMsg], max_deliver: int = 5) -> int:
    ok, failed, dead = _counters()
    return await runtime._process_batch(
        handler, msgs, ok, failed, dead, SETTINGS, max_deliver, _ctx()
    )


class TestProcessBatch:
    async def test_success_acks(self) -> None:
        msg = FakeMsg({"id": "x"})

        assert await _batch(_noop, [msg]) == 0
        assert msg.acked and not msg.naked and not msg.termed

    async def test_event_carries_stream_seq(self) -> None:
        seen: list[Event] = []

        async def capture(event: Event, ctx: object) -> None:
            seen.append(event)

        await _batch(capture, [FakeMsg({"a": 1})])

        assert seen[0].stream_seq == 7

    async def test_failure_naks_and_counts(self) -> None:
        msg = FakeMsg({"id": "x"})

        assert await _batch(_boom, [msg]) == 1
        assert msg.naked and not msg.acked and not msg.termed

    async def test_poison_terminated_at_max_deliver(self) -> None:
        msg = FakeMsg({"id": "x"}, num_delivered=5)

        assert await _batch(_boom, [msg]) == 1
        assert msg.termed and not msg.naked

    async def test_server_max_deliver_wins(self) -> None:
        msg = FakeMsg({"id": "x"}, num_delivered=2)

        assert await _batch(_boom, [msg], max_deliver=2) == 1
        assert msg.termed

    async def test_permanent_error_terminates_immediately(self) -> None:
        async def perm(event: Event, ctx: object) -> None:
            raise PermanentError("bad payload")

        msg = FakeMsg({"id": "x"}, num_delivered=1)

        assert await _batch(perm, [msg]) == 1
        assert msg.termed and not msg.naked

    async def test_hanging_handler_times_out_to_nak(self) -> None:
        async def hang(event: Event, ctx: object) -> None:
            await asyncio.sleep(60)

        msg = FakeMsg({"id": "x"})
        settings = Settings(
            name="fn-x",
            stream="COMMS",
            subject="s",
            consumer="fn-x",
            handler_timeout_s=0.01,
        )
        ok, failed, dead = _counters()

        failures = await runtime._process_batch(
            hang, [msg], ok, failed, dead, settings, 5, _ctx()
        )

        assert failures == 1 and msg.naked

    async def test_settle_failure_does_not_abort_batch(self) -> None:
        """A failing ack must not escape and kill the drain (exit-0)."""
        bad = FakeMsg({"id": "a"}, fail_settle=True)
        good = FakeMsg({"id": "b"})

        failures = await _batch(_noop, [bad, good])

        assert failures == 0  # handler succeeded on both
        assert good.acked and not bad.acked


class FakeSub:
    """Scripted fetch outcomes: lists are batches, exceptions raised."""

    def __init__(self, script: list[Any]) -> None:
        self.script = list(script)

    async def fetch(self, batch: int, timeout: float = 0) -> list[FakeMsg]:
        item = self.script.pop(0) if self.script else asyncio.TimeoutError()
        if isinstance(item, BaseException):
            raise item
        return item  # type: ignore[no-any-return]


class FakeJs:
    def __init__(self, sub: FakeSub, ack_pending: list[int]) -> None:
        self._sub = sub
        self._ack_pending = list(ack_pending)
        self._limits_read = False

    async def pull_subscribe_bind(self, consumer: str, stream: str) -> FakeSub:
        return self._sub

    async def consumer_info(self, stream: str, consumer: str) -> Any:
        # First call comes from _consumer_limits and must not consume
        # the scripted ack-pending sequence.
        if not self._limits_read:
            self._limits_read = True
            pending = 0
        else:
            pending = self._ack_pending.pop(0) if self._ack_pending else 0
        return SimpleNamespace(
            num_ack_pending=pending, config=SimpleNamespace(max_deliver=5)
        )


class FakeNc:
    def __init__(self, js: FakeJs) -> None:
        self._js = js
        self.closed = False

    def jetstream(self) -> FakeJs:
        return self._js

    async def close(self) -> None:
        self.closed = True


def _wire(
    monkeypatch: pytest.MonkeyPatch, sub: FakeSub, ack_pending: list[int]
) -> FakeNc:
    nc = FakeNc(FakeJs(sub, ack_pending))

    async def fake_connect(settings: Settings) -> FakeNc:
        return nc

    async def no_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr(runtime, "_connect", fake_connect)
    monkeypatch.setattr(runtime.asyncio, "sleep", no_sleep)
    return nc


class TestDrain:
    async def test_empty_consumer_exits_clean(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nc = _wire(monkeypatch, FakeSub([asyncio.TimeoutError()]), [0])

        failures = await runtime.drain(_noop, SETTINGS)

        assert failures == 0 and nc.closed

    async def test_processes_then_waits_for_ack_pending(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Nak'd work is waited for, then drained, then clean exit."""
        redelivered = FakeMsg({"r": 1})
        sub = FakeSub(
            [
                [FakeMsg({"a": 1})],
                asyncio.TimeoutError(),  # ack-pending 1 -> wait
                [redelivered],
                asyncio.TimeoutError(),  # ack-pending 0 -> exit
            ]
        )
        _wire(monkeypatch, sub, [1, 0])

        failures = await runtime.drain(_noop, SETTINGS)

        assert failures == 0 and redelivered.acked

    async def test_budget_resets_on_progress(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Idle before a batch must not consume the post-batch budget."""
        clock = {"t": 0.0}

        class FakeLoop:
            def time(self) -> float:
                clock["t"] += runtime.ACK_PENDING_WAIT_BUDGET_S / 2 + 1
                return clock["t"]

        monkeypatch.setattr(runtime.asyncio, "get_running_loop", lambda: FakeLoop())
        # idle (starts budget), idle (near-exhausts), batch (resets),
        # idle (new budget), idle, then pending drops to 0
        sub = FakeSub(
            [
                asyncio.TimeoutError(),
                asyncio.TimeoutError(),
                [FakeMsg({"a": 1})],
                asyncio.TimeoutError(),
                asyncio.TimeoutError(),
                asyncio.TimeoutError(),
            ]
        )
        _wire(monkeypatch, sub, [1, 1, 1, 1, 0])

        failures = await runtime.drain(_noop, SETTINGS)

        # post-batch wait happened (script consumed past the batch)
        assert failures == 0
        assert not sub.script

    async def test_connection_loss_counts_and_exits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import nats.errors

        sub = FakeSub([nats.errors.ConnectionClosedError()])
        nc = _wire(monkeypatch, sub, [])

        failures = await runtime.drain(_noop, SETTINGS)

        assert failures == 1 and nc.closed


class TestMainDispatch:
    def _env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FN_NAME", "fn-x")
        monkeypatch.setenv("FN_SUBJECT", "comms.a.b")
        monkeypatch.setenv("FN_CONSUMER", "fn-x")
        monkeypatch.setenv("NATS_TOKEN", "t")

    def test_help_needs_no_config(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.delenv("FN_SUBJECT", raising=False)

        runtime.main(_noop, argv=["--help"])

        assert "usage" in capsys.readouterr().out

    def test_unknown_command_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._env(monkeypatch)

        with pytest.raises(SystemExit, match="unknown command"):
            runtime.main(_noop, argv=["bogus"])

    def test_missing_config_exits_with_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FN_SUBJECT", raising=False)
        monkeypatch.delenv("FN_CONSUMER", raising=False)
        monkeypatch.delenv("FN_NAME", raising=False)

        with pytest.raises(SystemExit, match="must be set"):
            runtime.main(_noop, argv=["drain"])

    def test_drain_swallows_infrastructure_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The exit-0 contract: drain never SystemExits on infra errors."""
        self._env(monkeypatch)

        async def explode(handler: Any, settings: Settings) -> int:
            raise RuntimeError("nats down")

        monkeypatch.setattr(runtime, "drain", explode)

        runtime.main(_noop, argv=["drain"])  # must not raise

    def test_init_dispatches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._env(monkeypatch)
        called = {}

        async def fake_init(settings: Settings) -> None:
            called["settings"] = settings

        monkeypatch.setattr(runtime, "init_consumer", fake_init)

        runtime.main(_noop, argv=["init"])

        assert called["settings"].consumer == "fn-x"

    def test_lowercase_log_level_accepted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._env(monkeypatch)
        monkeypatch.setenv("LOG_LEVEL", "debug")

        async def fake_drain(handler: Any, settings: Settings) -> int:
            return 0

        monkeypatch.setattr(runtime, "drain", fake_drain)

        runtime.main(_noop, argv=["drain"])  # must not raise


class TestSettings:
    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FN_NAME", "fn-x")
        monkeypatch.setenv("FN_SUBJECT", "comms.a.b")
        monkeypatch.setenv("FN_CONSUMER", "fn-x")

        settings = Settings.from_env()
        settings.validate()

        assert settings.stream == "COMMS"
        assert settings.batch_size == 10

    def test_name_falls_back_to_consumer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("FN_NAME", raising=False)
        monkeypatch.setenv("FN_SUBJECT", "s")
        monkeypatch.setenv("FN_CONSUMER", "fn-y")

        assert Settings.from_env().name == "fn-y"

    def test_empty_stream_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FN_STREAM", "")
        monkeypatch.setenv("FN_SUBJECT", "s")
        monkeypatch.setenv("FN_CONSUMER", "c")
        monkeypatch.setenv("FN_NAME", "n")

        with pytest.raises(ConfigError, match="stream"):
            Settings.from_env().validate()

    def test_non_numeric_value_is_config_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FN_HANDLER_TIMEOUT_S", "60s")

        with pytest.raises(ConfigError, match="non-numeric"):
            Settings.from_env()

    def test_timeout_must_stay_under_ack_wait(self) -> None:
        settings = Settings(
            name="n",
            stream="COMMS",
            subject="s",
            consumer="c",
            handler_timeout_s=120.0,
        )

        with pytest.raises(ConfigError, match="ack_wait"):
            settings.validate()


class TestConnect:
    async def test_requires_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("NATS_TOKEN", raising=False)

        with pytest.raises(ConfigError):
            await runtime._connect(SETTINGS)


class TestPushMetrics:
    def test_disabled_without_url(self) -> None:
        runtime._push_metrics(CollectorRegistry(), SETTINGS)  # no raise

    def test_unreachable_gateway_swallowed(self) -> None:
        settings = Settings(
            name="fn-x",
            stream="COMMS",
            subject="s",
            consumer="c",
            pushgateway_url="http://127.0.0.1:1",
        )

        runtime._push_metrics(CollectorRegistry(), settings)  # no raise


class TestHandlerContext:
    async def test_publish_serializes_and_sets_msg_id(self) -> None:
        calls: list[tuple[str, bytes, dict[str, str] | None]] = []

        class PubJs:
            async def publish(
                self,
                subject: str,
                payload: bytes,
                headers: dict[str, str] | None = None,
            ) -> None:
                calls.append((subject, payload, headers))

        js: Any = PubJs()
        ctx = HandlerContext(js, SETTINGS, CollectorRegistry())

        await ctx.publish("comms.fn.x.out", {"a": 1}, msg_id="x:1")
        await ctx.publish("comms.fn.x.out", {"b": 2})

        assert calls[0] == (
            "comms.fn.x.out",
            b'{"a":1}',
            {"Nats-Msg-Id": "x:1"},
        )
        assert calls[1][2] is None

    async def test_counter_registers_on_pushed_registry(self) -> None:
        registry = CollectorRegistry()
        js: Any = object()
        ctx = HandlerContext(js, SETTINGS, registry)

        counter = ctx.counter("fn_custom_total", "custom")
        counter.inc()

        assert registry.get_sample_value("fn_custom_total") == 1.0


class TestEvent:
    def test_json_round_trip(self) -> None:
        event = Event(subject="s", data=b'{"k": 1}', num_delivered=1)

        assert event.json() == {"k": 1}

    def test_non_object_rejected(self) -> None:
        event = Event(subject="s", data=b"[1]", num_delivered=1)

        with pytest.raises(ValueError):
            event.json()
