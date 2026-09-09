from retry import process_with_retry, TransientError


def test_process_with_retry_succeeds_first_try():
    sleeps = []

    def func():
        return "ok"

    success, attempts, result = process_with_retry(
        func, max_retries=3, backoff_seconds=0.1, sleep_fn=sleeps.append
    )

    assert success is True
    assert attempts == 1
    assert result == "ok"
    assert sleeps == []


def test_process_with_retry_succeeds_after_two_failures():
    sleeps = []
    call_count = {"n": 0}

    def func():
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise TransientError("flaky")
        return "ok"

    success, attempts, result = process_with_retry(
        func, max_retries=3, backoff_seconds=0.1, sleep_fn=sleeps.append
    )

    assert success is True
    assert attempts == 3
    assert result == "ok"
    assert sleeps == [0.1, 0.2]


def test_process_with_retry_gives_up_after_max_retries():
    sleeps = []

    def func():
        raise TransientError("always fails")

    success, attempts, result = process_with_retry(
        func, max_retries=3, backoff_seconds=0.1, sleep_fn=sleeps.append
    )

    assert success is False
    assert attempts == 4
    assert result is None
    assert sleeps == [0.1, 0.2, 0.4]
