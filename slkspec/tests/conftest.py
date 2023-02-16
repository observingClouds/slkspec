"""pytest definitions to run the unittests."""
from __future__ import annotations

import shutil
from pathlib import Path
from subprocess import PIPE, run
from tempfile import TemporaryDirectory
from typing import Generator

import mock
import numpy as np
import pandas as pd
import pytest
import xarray as xr


class SLKMock:
    """A mock that emulates what pyslk is doing."""

    def __init__(self, _cache: dict[int, list[str]] = {}) -> None:
        self._cache = _cache

    def list(self, inp_path: str) -> str:
        """Mock the slk_list method."""
        res = (
            run(["ls", "-l", inp_path], stdout=PIPE, stderr=PIPE)
            .stdout.decode()
            .split("\n")
        )
        return "\n".join(res[1:] + [res[0]])

    def search(self, inp_f: list[str]) -> int:
        """Mock slk_search."""
        if not inp_f:
            return None
        hash_value = abs(hash(",".join(inp_f)))
        self._cache[hash_value] = inp_f
        return hash_value

    def slk_gen_file_query(self, inp_files: list[str]) -> list[str]:
        """Mock slk_gen_file_qeury."""
        return [f for f in inp_files if Path(f).exists()]

    def slk_retrieve(self, search_id: int, out_dir: str) -> None:
        """Mock slk_retrieve."""
        for inp_file in map(Path, self._cache[search_id]):
            shutil.copy(inp_file, Path(out_dir) / inp_file.name)


def create_data(variable_name: str, size: int) -> xr.Dataset:
    """Create a xarray dataset."""
    coords: dict[str, np.ndarray] = {}
    coords["x"] = np.linspace(-10, -5, size)
    coords["y"] = np.linspace(120, 125, size)
    lat, lon = np.meshgrid(coords["y"], coords["x"])
    lon_vec = xr.DataArray(lon, name="Lg", coords=coords, dims=("y", "x"))
    lat_vec = xr.DataArray(lat, name="Lt", coords=coords, dims=("y", "x"))
    coords["time"] = np.array(
        [
            np.datetime64("2020-01-01T00:00"),
            np.datetime64("2020-01-01T12:00"),
            np.datetime64("2020-01-02T00:00"),
            np.datetime64("2020-01-02T12:00"),
        ]
    )
    dims = (4, size, size)
    data_array = np.empty(dims)
    for time in range(dims[0]):
        data_array[time] = np.zeros((size, size))
    dset = xr.DataArray(
        data_array,
        dims=("time", "y", "x"),
        coords=coords,
        name=variable_name,
    )
    data_array = np.zeros(dims)
    return xr.Dataset({variable_name: dset, "Lt": lon_vec, "Lg": lat_vec}).set_coords(
        list(coords.keys())
    )


@pytest.fixture(scope="session")
def patch_dir() -> Generator[Path, None, None]:
    with TemporaryDirectory() as temp_dir:
        with mock.patch("slkspec.core.pyslk", SLKMock()):
            yield Path(temp_dir)


@pytest.fixture(scope="session")
def save_dir() -> Generator[Path, None, None]:
    """Create a temporary directory."""
    with TemporaryDirectory() as td:
        yield Path(td)


@pytest.fixture(scope="session")
def data() -> Generator[xr.Dataset, None, None]:
    """Define a simple dataset with a blob in the middle."""
    dset = create_data("precip", 100)
    yield dset


@pytest.fixture(scope="session")
def netcdf_files(
    data: xr.Dataset,
) -> Generator[Path, None, None]:
    """Save data with a blob to file."""

    with TemporaryDirectory() as td:
        for time in (data.time[:2], data.time[2:]):
            time1 = pd.Timestamp(time.values[0]).strftime("%Y%m%d%H%M")
            time2 = pd.Timestamp(time.values[1]).strftime("%Y%m%d%H%M")
            out_file = (
                Path(td)
                / "the_project"
                / "test1"
                / "precip"
                / f"precip_{time1}-{time2}.nc"
            )
            out_file.parent.mkdir(exist_ok=True, parents=True)
            data.sel(time=time).to_netcdf(out_file)
        yield Path(td)


@pytest.fixture(scope="session")
def zarr_file(
    data: xr.Dataset,
) -> Generator[Path, None, None]:
    """Save data with a blob to file."""

    with TemporaryDirectory() as td:
        out_file = Path(td) / "the_project" / "test1" / "precip" / "precip.zarr"
        out_file.parent.mkdir(exist_ok=True, parents=True)
        data.to_zarr(out_file)
        yield Path(td)
