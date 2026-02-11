"""Auto-discovery source registry with decorator-based registration.

Sources register themselves by decorating their ``fetch()`` function with
``@register_source("name")``.  The registry is populated at import time
when ``_discover_sources()`` walks the package with ``pkgutil``.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

# Callable type: async (config, date, **kwargs) -> dict
FetchFunc = Callable[..., Coroutine[Any, Any, dict]]

SOURCE_REGISTRY: dict[str, FetchFunc] = {}


def register_source(name: str) -> Callable[[FetchFunc], FetchFunc]:
    """Decorator that registers a source fetch function by name.

    Usage::

        @register_source("parliament")
        async def fetch(config, date, *, client=None):
            ...
    """

    def decorator(func: FetchFunc) -> FetchFunc:
        if name in SOURCE_REGISTRY:
            raise ValueError(f"Duplicate source registration: '{name}'")
        SOURCE_REGISTRY[name] = func
        return func

    return decorator
