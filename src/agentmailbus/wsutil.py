"""WebSocket entry points that work on the 13.x and 15.x APIs."""

from __future__ import annotations

try:
    from websockets.asyncio.client import connect as ws_connect
    from websockets.asyncio.server import serve as ws_serve
except ImportError:  # websockets 13
    from websockets import connect as ws_connect
    from websockets import serve as ws_serve

__all__ = ["ws_connect", "ws_serve"]
