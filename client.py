import multiprocessing as mp
import functools
import typing as t
import pyuda


class UDAClient:
    def __init__(self) -> None:
        self.client = pyuda.Client()

    def get_signal(self, name: str, shot: int):
        try:
            signal = self.client.get(name, shot)
        except pyuda.ServerException as exception:
            print(exception)
            signal = None
        return signal

    def get_signal_names(self, shot: int):
        return self.client.list_signals(shot)


class UDADatasetClient:
    def __init__(self) -> None:
        self.client = UDAClient()

    def get_signal_metadata(self, shots: t.List[int]):
        names = set()
        signal_metadata = []
        for shot in shots:
            items = self.client.get_signal_names(shot)
            for item in items:
                if item.signal_name not in names:
                    names.add(item.signal_name)
                    signal_metadata.append(item)

        return signal_metadata

    def get_signals(self, name: str, shots: t.List[int], num_workers: int = 8):
        _get_signal = functools.partial(self.client.get_signal, name)

        pool = mp.Pool(num_workers)
        for signal in pool.map(_get_signal, shots):
            yield signal
