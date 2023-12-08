import re
import numpy as np
import xarray as xr
import multiprocessing as mp
import functools
import typing as t

def normalize_dimension_names(signal):
    """Make the dimension names sensible"""
    dims = [dim.label for dim in signal.dims]
    count = 0
    dim_names = []

    name_mappings = {
        'Chord #': 'chord_number',
        'Radius': 'radius',
        'time': 'time',
        'Time': 'time',
        'Time (sec)': 'time',
    }

    empty_names = ['', ' ', '-']

    for name in dims:
        #if name not in name_mappings and name not in empty_names:
        #    raise RuntimeError(f'Unknown dimension name: {name}')

        # Create names for unlabelled dims
        if name in empty_names:
            name = f'dim_{count}'
            count += 1

        # Normalize weird names to standard names
        name = name_mappings.get(name, name)
        dim_names.append(name)

    dim_names = list(map(lambda x: x.lower(), dim_names))
    dim_names = [re.sub('[^a-zA-Z0-9_\n\.]', '', dim) for dim in dim_names]
    return dim_names


class UDAClient:
    def __init__(self) -> None:
        import pyuda
        self.client = pyuda.Client()

    def get_signal(self, name: str, shot: int):
        import pyuda
        try:
            signal = self.client.get(name, shot)
            signal = self.to_xarray(signal)
            return signal
        except pyuda.ServerException as exception:
            print(shot, exception)
            signal = None
            return None


    def to_xarray(self, signal):
        dim_names = normalize_dimension_names(signal)
        coords = {name: xr.DataArray(np.atleast_1d(dim.data), 
                                        dims=[name], 
                                        attrs=dict(units=dim.units)) 
                                        for name, dim in zip(dim_names, signal.dims)}

        data = np.atleast_1d(signal.data)
        errors = np.atleast_1d(signal.errors)

        data_vars = dict(data=xr.DataArray(data, dims=dim_names), error=xr.DataArray(errors, dims=dim_names))
        dataset = xr.Dataset(data_vars, coords=coords)
        return dataset

    def get_signal_names(self, shot: int):
        return self.client.list_signals(shot=shot)

def get_signal(name, shot):
    client = UDAClient()
    return client.get_signal(name, shot)

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
        _get_signal = functools.partial(get_signal, name)

        pool = mp.Pool(num_workers)
        for shot, signal in zip(shots, pool.map(_get_signal, shots)):
            yield shot, signal
