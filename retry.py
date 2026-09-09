import time
from typing import Callable, Optional, Tuple, TypeVar

T = TypeVar("T")


class TransientError(Exception):
    """Raised to simulate a flaky downstream dependency."""


def process_with_retry(
    func: Callable[[], T],
    max_retries: int = 3,
    backoff_seconds: float = 0.5,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Tuple[bool, int, Optional[T]]:
    attempts = 0
    while True:
        attempts += 1
        try:
            result = func()
            return True, attempts, result
        except TransientError:
            if attempts > max_retries:
                return False, attempts, None
            sleep_fn(backoff_seconds * (2 ** (attempts - 1)))
