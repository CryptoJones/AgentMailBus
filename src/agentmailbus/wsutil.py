"""WebSocket entry points. `websockets.asyncio` is the API since 13.0, the floor of the pin."""

from __future__ import annotations

from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import serve as ws_serve

__all__ = ["ws_connect", "ws_serve"]
