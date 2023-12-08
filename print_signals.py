import zarr
import shutil
import multiprocessing as mp
from client import UDADatasetClient

def write_file(item):
    shot, signal = item
    if signal is None:
        return shot
    signal.to_zarr('ip.zarr', group=f'{shot}')
    return shot
        

client = UDADatasetClient()
shots = list(range(30120, 30530))

signals = client.get_signals("ip", shots)

pool = mp.Pool(8)
names = []
for item in pool.map(write_file, signals):
    print(item)


shutil.make_archive('ip.zarr', 'zip', 'ip.zarr')
shutil.rmtree('ip.zarr')


