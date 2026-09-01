"""hyperlab-fn-runtime: shared drain runtime for hyperlab functions."""

from importlib.metadata import PackageNotFoundError, version

from hyperlab_fn_runtime.models import Event
from hyperlab_fn_runtime.runtime import (
    ConfigError,
    Handler,
    HandlerContext,
    PermanentError,
    Settings,
    drain,
    init_consumer,
    main,
)

try:
    __version__ = version("hyperlab-fn-runtime")
except PackageNotFoundError:  # source tree without an install
    __version__ = "0.0.0.dev0"

__all__ = [
    "ConfigError",
    "Event",
    "Handler",
    "HandlerContext",
    "PermanentError",
    "Settings",
    "__version__",
    "drain",
    "init_consumer",
    "main",
]
