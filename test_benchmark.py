from uda_parallel import get_signals_serial, get_signals_mp

EXPECTED_NUM_SIGNALS = 99

def test_serial(benchmark):

    def _do_get_signals():
        signals = get_signals_serial(30420)
        signals = filter(lambda x: x is not None, signals)
        return len(list(signals))

    result = benchmark.pedantic(_do_get_signals, rounds=1, iterations=1)
    assert result == EXPECTED_NUM_SIGNALS
    
def test_mp_with_shared_client(benchmark):

    def _do_get_signals():
        signals = get_signals_mp(30420, shared_client=True)
        signals = filter(lambda x: x is not None, signals)
        return len(list(signals))

    result = benchmark.pedantic(_do_get_signals, rounds=1, iterations=1)
    assert result == EXPECTED_NUM_SIGNALS

def test_mp_with_parallel_clients(benchmark):

    def _do_get_signals():
        signals = get_signals_mp(30420, shared_client=False)
        signals = filter(lambda x: x is not None, signals)
        return len(list(signals))

    result = benchmark.pedantic(_do_get_signals, rounds=1, iterations=1)
    assert result == EXPECTED_NUM_SIGNALS
