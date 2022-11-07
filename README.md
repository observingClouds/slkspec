# slkspec

This is work in progress!

```python
import fsspec

with fsspec.open("slk://arch/project/user/file", "r") as f:
    print(f.read())
```

## Usage in combination with preffs

Set location for tape archive retrievals on local machine
```bash
export SLK_CACHE="/path/to/local/cache/directory/"
```

Open parquet referenced zarr-file
```python
import xarray as xr
ds=xr.open_zarr(f"preffs::/path/to/preffs/data.preffs", storage_options={"preffs":{"prefix":"slk:///arch/<project>/<user>/slk/archive/prefix/"}
```
Now only those files are retrieved from tape which are needed for any requested dataset operation. In the beginning only the file containing the metadata (e.g. .zattrs, .zmetadata) and coordinates are requested (e.g. time). After the files have been retrieved once, they are saved at the path given in `SLK_CACHE` and accessed directly from there.
