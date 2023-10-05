# UDA Parallel Benchmarks

## Setup

```
/usr/local/depot/Python-3.9/bin/python3 -m venv mast
source ./mast/bin/activate
module switch uda/2.7.2
pip install -r requirements.txt
```


## Run

```
pytest test_benchmark.py
```

Run a single test:

```
pytest test_benchmark.py - k test_serial
```


