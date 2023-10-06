import time
import functools
import multiprocessing as mp
import pyuda
from mast.mast_client import ListType

DELAY = 1.0
MAX_SIGNALS = 100

def get_signal(name, client, shot, delay=None):
    if client is None:
        client = pyuda.Client()

    if delay is not None:
        time.sleep(delay)

    try:

        signal = client.get(name, shot)
    except pyuda.ServerException as exception:
        print(exception)
        signal = None
    return signal

def get_signals_serial(shot: int):
    client = pyuda.Client()
    signal_items = client.list(ListType.SIGNALS, shot)
    signal_items = signal_items[:MAX_SIGNALS]
    names = [signal_item.signal_name for signal_item in signal_items]

    for name in names:
        yield get_signal(name, client, shot)

def get_signals_mp(shot: int, shared_client: bool):
    client = pyuda.Client()
    signal_items = client.list(ListType.SIGNALS, shot)
    signal_items = signal_items[:MAX_SIGNALS]
    names = [signal_item.signal_name for signal_item in signal_items]

    c = client if not shared_client else None
    
    _get_signal = functools.partial(get_signal, client=c, shot=shot, delay=DELAY)

    pool = mp.Pool(8)
    for signal in pool.map(_get_signal, names):
        yield signal



