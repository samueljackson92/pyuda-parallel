import pyuda
import functools
import multiprocessing as mp
from mast.mast_client import ListType

# Limit the number of signals so it doesn't take too long to run
MAX_SIGNALS = 100

# This is how many signals we get back if we get the signals serially.
# We should get the same number of shots when using multi-processing
EXPECTED_NUM_SIGNALS = 99


def get_signal(name, shot):
    client = pyuda.Client()
    try:
        signal = client.get(name, shot)
    except pyuda.ServerException as e:
        signal = None
    return signal


def get_names(shot):
    client = pyuda.Client()
    signal_items = client.list(ListType.SIGNALS, shot)
    signal_items = signal_items[:MAX_SIGNALS]
    names = [signal_item.signal_name for signal_item in signal_items]
    return names


def get_signals_serial(shot: int):
    names = get_names(shot)
    for name in names:
        yield get_signal(name, shot)


def get_signals_mp(shot: int):
    names = get_names(shot)

    _get_signal = functools.partial(get_signal, shot=shot)

    pool = mp.Pool(8)
    for signal in pool.map(_get_signal, names):
        yield signal


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
