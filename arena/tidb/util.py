from threading import Lock


class AutoIDAllocator:
    def __init__(self):
        self._lock = Lock()
        self._idx = 0

    def alloc(self) -> int:
        with self._lock:
            self._idx += 1
            return self._idx
