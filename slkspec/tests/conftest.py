"""pytest definitions to run the unittests."""

from __future__ import annotations

import builtins
import inspect
import shutil
from datetime import datetime
from pathlib import Path
from subprocess import PIPE, run
from tempfile import TemporaryDirectory
from typing import Generator, Optional, Union

import mock
import numpy as np
import pandas as pd
import pytest
import xarray as xr

PYSLK_DEFAULT_LIST_COLUMNS = [
    "permissions",
    "owner",
    "group",
    "filesize",
    "timestamp",
    "filename",
]


class SLKMock:
    """A mock that emulates what pyslk is doing."""

    class StatusJob:
        """mock the pyslk.StatusJob"""

        PAUSED, QUEUED, PROCESSING, COMPLETED, SUCCESSFUL, FAILED, ABORTED = range(
            -4, 3
        )
        STATI = {}

        # PROCESSING includes COMPLETING
        # ABORTED includes ABORTING

        def __init__(self, status_str: str):
            """
            Converts a string representing the status of a recall job into a
            status_job object

            Possible values for status_str are:
                PAUSED, PAUSING, QUEUED, PROCESSING, COMPLETED, COMPLETING,
                ABORTED, ABORTING

            :param status_str: status of a recall job
            :type status_str: str
            """
            if not isinstance(status_str, str):
                raise TypeError(
                    f"pyslk.StatusJob.{inspect.stack()[0][3]}: wrong type "
                    + "provided; need 'str'; "
                    + f"got '{type(status_str).__name__}'"
                )

            self.status: int

            self.STATI[self.PAUSED] = "PAUSED"
            self.STATI[self.QUEUED] = "QUEUED"
            self.STATI[self.PROCESSING] = "PROCESSING"
            self.STATI[self.COMPLETED] = "COMPLETED"
            self.STATI[self.SUCCESSFUL] = "SUCCESSFUL"
            self.STATI[self.FAILED] = "FAILED"
            self.STATI[self.ABORTED] = "ABORTED"

            if status_str == "SUCCESSFUL":
                self.status = self.SUCCESSFUL
            elif status_str == "FAILED":
                self.status = self.FAILED
            elif status_str == "COMPLETED":
                self.status = self.COMPLETED
            elif status_str in ["PROCESSING", "COMPLETING"]:
                self.status = self.PROCESSING
            elif status_str[0:6] == "QUEUED":
                self.status = self.QUEUED
            elif status_str in ["PAUSED", "PAUSING"]:
                self.status = self.PAUSED
            elif status_str in ["ABORTED", "ABORTING"]:
                self.status = self.ABORTED
            else:
                raise ValueError(
                    f"pyslk.StatusJob.{inspect.stack()[0][3]}: provided "
                    + f"status cannot be processed: {status_str}; "
                    + "please contact support@dkrz.de"
                )

        def get_status(self) -> int:
            """
            return the status as integer

            Meaning of the output
            * -4: PAUSED / PAUSING
            * -3: QUEUED
            * -2: PROCESSING / COMPLETING
            * -1: COMPLETED
            * 0: SUCCESSFUL
            * 1: FAILED
            * 2: ABORTED / ABORTING

            :return: status as integer value (-4 to 2)
            :rtype: int
            """
            return self.status

        def get_status_name(self) -> str:
            return self.__str__()

        def __str__(self) -> str:
            return self.STATI[self.status]

        def get_possible_stati(self) -> dict:
            return self.STATI

        def is_paused(self) -> bool:
            return self.status == self.PAUSED

        def is_queued(self) -> bool:
            return self.status == self.QUEUED

        def is_processing(self) -> bool:
            return self.status == self.PROCESSING

        def is_finished(self) -> bool:
            return self.status in [
                self.COMPLETED,
                self.SUCCESSFUL,
                self.FAILED,
                self.ABORTED,
            ]

        def is_successful(self) -> bool:
            return self.status == self.SUCCESSFUL

        def is_completed(self) -> bool:
            return self.status == self.COMPLETED

        def has_failed(self) -> bool:
            return self.status in [self.FAILED, self.ABORTED]

        def __eq__(self, other: any) -> bool:
            if isinstance(other, "StatusJob"):
                return self.status == other.get_status()
            if isinstance(other, int):
                return self.status == other
            raise TypeError(
                f"pyslk.StatusJob.{inspect.stack()[0][3]}: wrong type "
                + "provided; need 'int' or 'StatusJob'; "
                + f"got '{type(other).__name__}'"
            )

        def __ne__(self, other: any) -> bool:
            if isinstance(other, "StatusJob"):
                return self.status != other.get_status()
            if isinstance(other, int):
                return self.status != other
            raise TypeError(
                f"pyslk.StatusJob.{inspect.stack()[0][3]}: wrong type "
                + "provided; need 'int' or 'StatusJob'; "
                + f"got '{type(other).__name__}'"
            )

    tape_barcode2id: dict[str, int] = {
        "M12345M8": 12345,
        "M23456M8": 23456,
        "M34567M8": 34567,
    }
    tape_status_default: dict[int, str] = {
        "M12345M8": "AVAILABLE",
        "M23456M8": "AVAILABLE",
        "M34567M8": "AVAILABLE",
    }
    tape_job_ids: dict[str, int] = {
        "M12345M8": 123456,
        "M23456M8": 234567,
        "M34567M8": 345678,
    }
    job_stati: dict[str, list[StatusJob]] = {
        "123456": [
            StatusJob("PROCESSING"),
            StatusJob("PROCESSING"),
            StatusJob("SUCCESSFUL"),
        ],
        "234567": [
            StatusJob("QUEUED"),
            StatusJob("PROCESSING"),
            StatusJob("PROCESSING"),
            StatusJob("SUCCESSFUL"),
        ],
        "345678": [
            StatusJob("PROCESSING"),
            StatusJob("PROCESSING"),
            StatusJob("SUCCESSFUL"),
        ],
    }
    tape_files: dict[str, list[str]] = {
        "M12345M8": [
            "/arch/ab1234/c567890/fileA.nc",
            "/arch/ab1234/c567890/fileB.nc",
            "/arch/ab1234/c567890/fileC.nc",
        ],
        "M23456M8": [
            "/arch/ab1234/c567890/fileD.nc",
            "/arch/ab1234/c567890/fileE.nc",
            "/arch/ab1234/c567890/fileF.nc",
        ],
        "M34567M8": [
            "/arch/ab1234/c567890/fileG.nc",
            "/arch/ab1234/c567890/fileH.nc",
            "/arch/ab1234/c567890/fileI.nc",
        ],
    }
    files_path2id: dict[str, int] = {
        "/arch/ab1234/c567890/fileA.nc": 40000001010,
        "/arch/ab1234/c567890/fileB.nc": 40000002010,
        "/arch/ab1234/c567890/fileC.nc": 40000003010,
        "/arch/ab1234/c567890/fileD.nc": 40000004010,
        "/arch/ab1234/c567890/fileE.nc": 40000005010,
        "/arch/ab1234/c567890/fileF.nc": 40000006010,
        "/arch/ab1234/c567890/fileG.nc": 40000007010,
        "/arch/ab1234/c567890/fileH.nc": 40000008010,
        "/arch/ab1234/c567890/fileI.nc": 40000009010,
    }

    def __init__(self, _cache: dict[int, builtins.list[str]] = {}) -> None:
        self._cache = _cache
        self.resource_tape: dict[str, str] = dict()
        self.files_special: list[str] = list()
        self.tape_special_ids: list[int] = list()
        for k, v in self.tape_files.items():
            for i in v:
                self.resource_tape[i] = k
                self.files_special.append(i)
        for k, v in self.tape_barcode2id.items():
            self.tape_special_ids.append(v)
        self.job_counters: dict[str, int] = dict()
        self.job_active: dict[str, bool] = dict()
        for k in self.job_counters.keys():
            self.job_counters[str(k)] = 0
            self.job_active[str(k)] = False
        # 80000000000 + resource_counter*1000
        self.resource_counter: int = 0
        # 400000 + job_counter
        self.job_counter: int = 0
        # 40000 + tape_counter
        self.tape_counter: int = 0
        self.resource_id2path: dict[str, str] = dict()
        self.resource_path2id: dict[str, int] = dict()
        for k, v in self.files_path2id.items():
            self.resource_id2path[str(v)] = k
            self.resource_path2id[k] = v

    def slk_list(self, inp_path: str) -> str:
        """Mock the slk_list method."""
        res = (
            run(["ls", "-l", inp_path], stdout=PIPE, stderr=PIPE)
            .stdout.decode()
            .split("\n")
        )
        return "\n".join(res[1:] + [res[0]])

    def search(self, inp_f: list[str]) -> int | None:
        """Mock slk_search."""
        if not inp_f:
            return None
        hash_value = abs(hash(",".join(inp_f)))
        self._cache[hash_value] = inp_f
        return hash_value

    def ls(
        self,
        path_or_id: Union[
            str,
            int,
            Path,
            list[str],
            list[int],
            list[Path],
            set[str],
            set[int],
            set[Path],
        ],
        show_hidden: bool = False,
        numeric_ids: bool = False,
        recursive: bool = False,
        column_names: list = PYSLK_DEFAULT_LIST_COLUMNS,
        parse_dates: bool = True,
        parse_sizes: bool = True,
        full_path: bool = True,
    ) -> pd.DataFrame:
        """Mock slk_ls."""
        return pd.DataFrame(
            [
                [
                    "-rwxr-xr-x-",
                    "k204221",
                    "bm0146",
                    1268945,
                    datetime.now(),
                    "/arch/bm0146/k204221/iow/INDEX.txt",
                ]
            ],
            columns=column_names,
        )

    def is_cached(self, resource_path: Union[Path, str]) -> bool:
        """Mock pyslk.is_cached."""
        if not self._are_resources_special(resource_path):
            return True
        else:
            tape_barcode: str = self.resource_tape[str(resource_path)]
            job_id: int = self.tape_job_ids[tape_barcode]
            if self.get_job_status(job_id).is_finished():
                return True
            else:
                return False

    def group_files_by_tape(
        self,
        resource_path: Union[Path, str, list, None] = None,
        resource_ids: Union[str, list, int, None] = None,
        search_id: Union[str, int, None] = None,
        search_query: Union[str, None] = None,
        recursive: bool = False,
        max_tape_number_per_search: int = -1,
        run_search_query: bool = False,
        evaluate_regex_in_input: bool = False,
    ) -> list[dict]:
        """Mock slk_group_files_by_tape."""
        if resource_path is None:
            raise ValueError("'None' for 'resource_path' not implemented in mock")
        if isinstance(resource_path, (Path, str)):
            return self.group_files_by_tape(
                resource_path=[resource_path],
                resource_ids=resource_ids,
                search_id=search_id,
                search_query=search_query,
                recursive=recursive,
                max_tape_number_per_search=max_tape_number_per_search,
                run_search_query=run_search_query,
                evaluate_regex_in_input=evaluate_regex_in_input,
            )
        # define output
        tmp_result: dict[str, dict] = dict()
        result: list[dict] = []
        # iterate resources
        for resource in resource_path:
            # check if file is cached
            if self.is_cached(resource):
                # if 'cached' entry in tmp_results => append resource
                # if not => create entry
                if "cached" in tmp_result:
                    tmp_result["cached"]["file_count"] = (
                        tmp_result["cached"]["file_count"] + 1
                    )
                    tmp_result["cached"]["files"].append(resource)
                    tmp_result["cached"]["file_ids"].append(
                        self.get_resource_id(resource)
                    )
                else:
                    tmp_result["cached"] = {
                        "id": -1,
                        "location": "cache",
                        "description": "files currently stored in the HSM cache",
                        "barcode": "",
                        "status": "",
                        "file_count": 1,
                        "files": [resource],
                        "file_ids": [self.get_resource_id(resource)],
                    }
            else:
                # this resource is not cached but has to be retrieved from a tape
                tape_barcode: str = self.resource_tape[str(resource)]
                if tape_barcode in tmp_result:
                    tmp_result[tape_barcode]["file_count"] = (
                        tmp_result[tape_barcode]["file_count"] + 1
                    )
                    tmp_result[tape_barcode]["files"].append(resource)
                    tmp_result[tape_barcode]["file_ids"].append(
                        self.get_resource_id(resource)
                    )
                else:
                    tmp_result[tape_barcode] = {
                        "id": self.get_tape_id(tape_barcode),
                        "location": "tape",
                        "description": "files currently stored in tape",
                        "barcode": tape_barcode,
                        "status": "",
                        "file_count": 1,
                        "files": [resource],
                        "file_ids": [self.get_resource_id(resource)],
                    }
        for v in tmp_result.values():
            result.append(v)

        return result

    def get_tape_status(self, tape: int | str) -> str | None:
        special_barcode: str | None = ""
        # check if input tape is in list of special tapes
        if isinstance(tape, str):
            if tape in self.tape_barcode2id.keys():
                special_barcode = tape
        elif isinstance(tape, int):
            if tape in self.tape_special_ids:
                special_barcode = self.get_tape_barcode[tape]
        if special_barcode is not None:
            job_id: int = self.tape_job_ids[special_barcode]
            # if job status indicates that the job is not finished,
            # then tape is blocked
            if not self._get_job_status(job_id).is_finished():
                return "BLOCKED"
        # all else:
        return "AVAILABLE"

    def _get_job_status(self, job_id: int) -> StatusJob | None:
        if str(job_id) in self.job_active and self.job_active[str(job_id)]:
            return self.job_stati[str(job_id)][self.job_counters[str(job_id)]]
        # default
        return self.StatusJob("SUCCESS")

    def get_job_status(self, job_id: int) -> StatusJob | None:
        tmp_job_status: self.StatusJob = self._get_job_status(job_id)
        # if job is active then increment the job status counter by one
        if str(job_id) in self.job_active and self.job_active[str(job_id)]:
            self.job_counter = self.job_counter + 1
            # set job to inactive when all stati were iterated
            if self.job_counter >= len(self.job_stati[str(job_id)]):
                self.job_active[str(job_id)] = False
        # StatusJob(
        return tmp_job_status

    def _are_resources_special(
        self,
        resources: (
            Path
            | str
            | int
            | list[Path]
            | list[str]
            | list[int]
            | set[Path]
            | set[str]
            | set[int]
        ),
    ) -> bool:
        output: bool = False
        if isinstance(resources, (Path, str, int)):
            resource_path: str
            if isinstance(resources, (Path, str)):
                resource_path = str(resources)
            else:
                resource_path = self.get_resource_path(resources)
            if resource_path in self.files_path2id:
                output = True
        else:
            for resource in resources:
                output = output or self._are_resources_special(resource)
        return output

    def _get_tape_of_special_resources(
        self,
        resources: (
            Path
            | str
            | int
            | list[Path]
            | list[str]
            | list[int]
            | set[Path]
            | set[str]
            | set[int]
        ),
    ) -> str:
        output: str
        tmp_tape: str
        if isinstance(resources, (Path, str, int)):
            resource_path: str
            if isinstance(resources, (Path, str)):
                resource_path = str(resources)
            else:
                resource_path = self.get_resource_path(resources)
            output = self.get_resource_tape(resource_path)
        else:
            for resource in resources:
                tmp_tape = self._get_tape_of_special_resources(resource)
                if output is None:
                    output = tmp_tape
                else:
                    if output != tmp_tape:
                        raise ValueError("All resources must be in the same tape")
        return output

    def recall_single(
        self,
        resources: (
            Path
            | str
            | int
            | list[Path]
            | list[str]
            | list[int]
            | set[Path]
            | set[str]
            | set[int]
        ),
        destination: Path | str | None = None,
        resource_ids: bool = False,
        search_id: bool = False,
        recursive: bool = False,
        preserve_path: bool = True,
    ) -> int:
        if not self._are_resources_special(resources):
            job_id: int = 300000 + self.job_counter
            self.job_counter = self.job_counter + 1
            return job_id
        else:
            return self.tape_job_ids[self._get_tape_of_special_resources(resources)]

    def get_tape_barcode(self, tape_id: int) -> str:
        return f"M{tape_id/10}M8"

    def get_tape_id(self, tape_barcode: str) -> int:
        return int(tape_barcode[1:6]) * 10

    def get_resource_tape(self, resource_path: str | Path) -> dict[int, str] | None:
        if str(resource_path) in self.resource_tape:
            return self.resource_tape[str(resource_path)]
        else:
            # construct a tape
            tmp_tape_id: int = 40000 + self.tape_counter
            tmp_tape_barcode: str = self.get_tape_barcode(tmp_tape_id)
            self.resource_tape[str(resource_path)] = tmp_tape_barcode
            return {tmp_tape_id: tmp_tape_barcode}

    def get_resource_path(self, resource_id: int) -> Path | None:
        if str(resource_id) in self.resource_id2path:
            return self.resource_id2path[str(resource_id)]
        else:
            raise ValueError(f"Resource id {resource_id} not found")

    def get_resource_id(self, resource_path: str | Path) -> int | None:
        if str(resource_path) in self.resource_path2id:
            return self.resource_path2id[str(resource_path)]
        else:
            # construct a resource
            tmp_resource_id: int = 80000000000 + self.resource_counter * 1000
            self.resource_counter = self.resource_counter + 1
            self.resource_path2id[str(resource_path)] = tmp_resource_id
            self.resource_id2path[str(tmp_resource_id)] = resource_path
            return tmp_resource_id

    def _resource_status(self, resource_path: Path | str) -> tuple[str, str]:
        if resource_path not in self.files_special:
            return "ENVISAGED", "ENVISAGED"
        else:
            tape: str = self.resource_tape[str(resource_path)]
            job_id: int = self.tape_job_ids[tape]
            if self._get_job_status(job_id).is_finished():
                return "ENVISAGED", "ENVISAGED"
            else:
                return "FAILED", "FAILED_NOT_CACHED"

    def _retrieve(
        self,
        resource: Path | str | int,
        destination: Path | str,
        dry_run: bool = False,
        force_overwrite: bool = False,
        ignore_existing: bool = False,
        resource_ids: bool = False,
        search_id: bool = False,
        recursive: bool = False,
        stop_on_failed_retrieval: bool = False,
        preserve_path: bool = True,
        verbose: bool = False,
    ) -> tuple[str, str, str, str, int]:
        status1: str
        status2: str
        resource_id: int
        resource_path: Path
        # get resource path/id
        if isinstance(resource, int):
            if resource_ids:
                resource_id = resource
                resource_path = self.get_resource_path(resource)
            else:
                raise ValueError(
                    "If resource_ids is False, resource must not be an int. "
                    + "Search ids are not implemented yet."
                )
        else:
            resource_id = self.get_resource_id(resource)
            resource_path = Path(resource)
            # modify status for special files
            status1, status2 = self._resource_status(resource_path)
        # set destination paths
        outfile: Path
        if preserve_path:
            outfile = Path(destination) / Path(
                str(resource_path).strip(resource_path.root)
            )
        else:
            outfile = Path(destination) / resource_path.name
        # check if destination exists:
        if outfile.exists() and status1 != "FAILED" and not force_overwrite:
            status1 = "SKIPPED"
            status2 = "SKIPPED_TARGET_EXISTS"
        # copy resource and change status
        if not dry_run and status1 in ["ENVISAGED"]:
            shutil.copy(resource_path, outfile)
            status1 = "SUCCESS"
            status2 = "SUCCESS"
        # return
        return status1, status2, str(resource_path), str(outfile), resource_id

    def retrieve_improved(
        self,
        resources: (
            Path
            | str
            | int
            | list[Path]
            | list[str]
            | list[int]
            | set[Path]
            | set[str]
            | set[int]
        ),
        destination: Path | str,
        dry_run: bool = False,
        force_overwrite: bool = False,
        ignore_existing: bool = False,
        resource_ids: bool = False,
        search_id: bool = False,
        recursive: bool = False,
        stop_on_failed_retrieval: bool = False,
        preserve_path: bool = True,
        verbose: bool = False,
    ) -> Optional[dict] | None:
        output: dict = dict()
        # structure of tmp_tuple
        #  (val0, val1, val2, val3, val4)
        #    val0: str => "ENVISAGED"|"SKIPPED"|"FAILED"|"SUCCESS"
        #    val1: str => "ENVISAGED"|"SKIPPED_TARGET_EXISTS"
        #                   |"FAILED_NOT_CACHED"|"SUCCESS"
        #    val2: str => resource path source
        #    val3: str => resource path destination
        #    val4: int => resource id
        tmp_tuple: tuple[str, str, str, str, int]
        if isinstance(resources, (Path, str, int)):
            tmp_tuple = self._retrieve(
                resources,
                destination,
                dry_run,
                force_overwrite,
                ignore_existing,
                resource_ids,
                search_id,
                recursive,
                stop_on_failed_retrieval,
                preserve_path,
                verbose,
            )
            output[tmp_tuple[0]] = {tmp_tuple[1]: [tmp_tuple[2]]}
            output["FILES"] = {tmp_tuple[2]: tmp_tuple[3]}
        elif isinstance(resources, (list, set)):
            output["FILES"] = dict()
            for resource in resources:
                tmp_tuple = self._retrieve(
                    resource,
                    destination,
                    dry_run,
                    force_overwrite,
                    ignore_existing,
                    resource_ids,
                    search_id,
                    recursive,
                    stop_on_failed_retrieval,
                    preserve_path,
                    verbose,
                )
                if tmp_tuple[0] not in output:
                    output[tmp_tuple[0]] = dict()
                if tmp_tuple[1] not in output[tmp_tuple[0]]:
                    output[tmp_tuple[0]][tmp_tuple[1]] = list()
                output[tmp_tuple[0]][tmp_tuple[1]].append(tmp_tuple[2])
                output["FILES"][tmp_tuple[2]] = tmp_tuple[3]
        return output

    class PySlkException(BaseException):
        pass


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
