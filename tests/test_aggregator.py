from aggregator import RunningAverage


def test_running_average_initial_zero():
    agg = RunningAverage()
    assert agg.count == 0
    assert agg.average == 0.0


def test_running_average_single_update():
    agg = RunningAverage()
    agg.update(10.0)
    assert agg.count == 1
    assert agg.average == 10.0


def test_running_average_multiple_updates():
    agg = RunningAverage()
    for price in (10.0, 20.0, 30.0):
        agg.update(price)
    assert agg.count == 3
    assert agg.average == 20.0
