import threading
from typing import Callable, Optional

_lock = threading.Lock()
_notifier: Optional[Callable[[], None]] = None


def set_notifier(fn: Optional[Callable[[], None]]):
    global _notifier
    with _lock:
        _notifier = fn


def notify():
    with _lock:
        fn = _notifier
    if fn:
        try:
            fn()
        except Exception:
            pass

