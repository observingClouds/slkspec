# slkspec

This is work in progress! This repository showcases how the tape archive can be integrated into the scientific workflow.

Pull requests are welcomed!

```python
import fsspec

with fsspec.open("slk://arch/project/user/file", "r") as f:
    print(f.read())
```

## Usage in combination with preffs
### Installation of additional requirements
```console
git clone git@github.com:observingClouds/slkspec.git
cd slkspec
mamba env create
mamba activate slkspec
pip install .
```

### Loading dataset

```python

import ffspec
import xarray as xr

url = fsspec.open("slk:////arch/project/file.nc", slk_cache="/scratch/b/b12346").open()
dset = xr.open_dataset(url)
```

## Current limitations (among others)
- tape retrievals are done within the currently selected compute resources, which might not be needed for a simple tape retrieval
