# hyperlab-fn-runtime

Shared drain runtime for hyperlab serverless functions: a KEDA
ScaledJob pod that binds a durable NATS JetStream pull consumer,
processes messages through your handler with at-least-once semantics
(bounded at 5 deliveries, then termination with a metric), and exits
when the consumer is empty.

Extracted from hyperlab-fn-template (HYP-90) so drain/ack/nak/metrics
semantics have ONE tested home; generated functions depend on this
package by git tag and ship only their handler.

## Usage (what the template generates)

```python
# <pkg>/cli.py
from hyperlab_fn_runtime import main
from <pkg>.handler import handle

def cli() -> None:
    main(handle)
```

```python
# <pkg>/handler.py
from hyperlab_fn_runtime import Event, HandlerContext

async def handle(event: Event, ctx: HandlerContext) -> None:
    payload = event.json()
    await ctx.publish("comms.fn.<name>.processed", {...}, msg_id="...")
```

Configuration is environment-driven (FN_NAME, FN_STREAM, FN_SUBJECT,
FN_CONSUMER, NATS_URL, NATS_TOKEN, PUSHGATEWAY_URL,
FN_HANDLER_TIMEOUT_S); deploy manifests are the single config source.

This repo is PUBLIC so generated repos' CI and image builds can
install it without credentials; it contains no lab secrets.

## Development

```bash
poetry install
poetry run pytest
poetry run mypy src
```

Releases: semver git tags (vX.Y.Z); consumers pin the tag.
