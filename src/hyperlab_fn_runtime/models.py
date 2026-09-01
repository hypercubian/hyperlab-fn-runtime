"""Event model handed to function handlers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Event:
    """One message from the stream."""

    subject: str
    data: bytes
    num_delivered: int
    stream_seq: int = 0

    def json(self) -> dict[str, Any]:
        """Decode the payload as JSON (raises ValueError if not JSON)."""
        parsed = json.loads(self.data)
        if not isinstance(parsed, dict):
            raise ValueError(f"payload is not a JSON object: {self.subject}")
        return parsed
