[![CI](https://github.com/observingClouds/slkspec/workflows/Tests/badge.svg?branch=main)](https://github.com/observingClouds/slkspec/actions?query=workflow%3ATests)
[![Linter](https://github.com/observingClouds/slkspec/workflows/Linter/badge.svg?branch=main)](https://github.com/observingClouds/slkspec/actions?query=workflow%3ALinter)

# slkspec

This is work in progress! This repository showcases how the tape archive can be integrated into the scientific workflow.

Pull requests are welcomed!

```python
import fsspec

with fsspec.open("slk:///arch/project/user/file", "r") as f:
    print(f.read())
```

## Loading datasets

### one file without manual creation of a filesystem

```python

import ffspec
import xarray as xr

url = fsspec.open("slk:////arch/project/file.nc", slk_cache="/scratch/b/b12346").open()
dset = xr.open_dataset(url)
```

### one file with manual creation of a filesystem

```python

import ffspec
import xarray as xr

fs = fsspec.filesystem('slk')
f = fs.open('slk:///arch/bm0146/k204221/testing/testing05/test01.nc')
ds = xr.open_dataset(f)
```

### multiple files

```python

import ffspec
import xarray as xr

fs = fsspec.filesystem('slk')
my_files = [
        'slk:///arch/bm0146/k204221/testing/testing14/test_netcdf_01.nc',
        'slk:///arch/bm0146/k204221/testing/testing11/test_netcdf_a.nc',
        'slk:///arch/bm0146/k204221/testing/testing05/test01.nc'
]
ds = xr.open_mfdataset([fs.open(f)  for f in my_files])
```

## Usage in combination with preffs
### Installation of additional requirements
```console
mamba env create
mamba activate slkspec
pip install .[preffs]
```

Open parquet referenced zarr-file
```python
import xarray as xr
ds = xr.open_zarr(f"preffs::/path/to/preffs/data.preffs",
                storage_options={"preffs":{"prefix":"slk:///arch/<project>/<user>/slk/archive/prefix/"}
```

Now only those files are retrieved from tape which are needed for any requested
dataset operation. In the beginning only the file containing the metadata
(e.g. .zattrs, .zmetadata) and coordinates are requested (e.g. time). After the
files have been retrieved once, they are saved at the path given in
`SLK_CACHE` and accessed directly from there.
