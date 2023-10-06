import time
import functools
import multiprocessing as mp
from mast.mast_client import ListType

DELAY = 1.0
MAX_SIGNALS = 100

def get_signal(name, client, shot, delay=None):
    import pyuda
    if client is None:
        client = get_client()

    if delay is not None:
        time.sleep(delay)

    try:

        signal = client.get(name, shot)
    except pyuda.ServerException as exception:
        print(exception)
        signal = None
    return signal

def get_client():
    import pyuda
    return pyuda.Client()

def get_names(shot):
    client = get_client()
    signal_items = client.list(ListType.SIGNALS, shot)
    signal_items = signal_items[:MAX_SIGNALS]
    names = [signal_item.signal_name for signal_item in signal_items]
    return names

def get_signals_serial(shot: int):
    import pyuda
    client = pyuda.Client()
    names = get_names(shot)
    for name in names:
        yield get_signal(name, client, shot)

def get_signals_mp(shot: int, shared_client: bool):
    names = get_names(shot)
    
    c = get_client() if not shared_client else None
    
    _get_signal = functools.partial(get_signal, client=c, shot=shot, delay=DELAY)

    pool = mp.Pool(8)
    for signal in pool.map(_get_signal, names):
        yield signal



