from uda_parallel import get_signals_serial, get_signals_mp

EXPECTED_NUM_SIGNALS = 99


def test_serial():

    signals = get_signals_serial(30420)
    signals = filter(lambda x: x is not None, signals)
    count = len(list(signals))

    assert count == EXPECTED_NUM_SIGNALS


def test_mp():
    signals = get_signals_mp(30420)
    signals = filter(lambda x: x is not None, signals)
    count = len(list(signals))

    assert count == EXPECTED_NUM_SIGNALS
