from __future__ import annotations

import io
import json
import logging
import os
import threading
import time
import warnings
from collections import defaultdict
from datetime import datetime
from getpass import getuser
from pathlib import Path
from queue import Queue
from typing import (
    IO,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypedDict,
    Union,
    overload,
)

import pandas as pd
import pyslk
from fsspec.spec import AbstractFileSystem

logger = logging.getLogger("slkspec")
logger.setLevel(logging.INFO)

MAX_RETRIES = 2
MAX_PARALLEL_RECALLS = 4
MAX_RETRIES_RECALL = 3
FileQueue: Queue[Tuple[str, str]] = Queue(maxsize=-1)
FileInfo = TypedDict("FileInfo", {"name": str, "size": int, "type": str})
TapeGroup = TypedDict(
    "TapeGroup",
    {
        "id": int,
        "location": str,
        "description": str,
        "barcode": str,
        "status": str,
        "file_count": int,
        "files": list[str],
        "file_ids": list[int],
    },
)
_retrieval_lock = threading.Lock()


class SLKFile(io.IOBase):
    """File handle for files on the hsm archive.

    Parameters
    ----------
    url: str
        Source path of the file that should be retrieved.
    local_file: str
        Destination path of the downloaded file.
    slk_cache: str | Path
        Destination of the temporary storage. This directory is used to
        retrieve data from tape.
    override: bool, default: False
        Override existing files
    touch: bool, default: False
        Update existing files on the temporary storage to prevent them
        from being deleted. // not necessary as they are read.
    mode: str, default: rb
        Specify the mode in which the files are opened

        'r'       open for reading (default)
        'b'       binary mode (default)
        't'       text mode
    file_permissions: int, default: 0o644
        Permission when creating files.
    dir_permissions: int, default: 0o3775
        Permission when creating directories.   **kwargs:
        Additional keyword arguments passed to the open file descriptor method.

    Example
    -------

    Use fsspec to open data stored on tape, temporary data will be downloaded
    to a central scratch folder:

    ::

        import ffspec
        import xarray as xr

        url = fsspec.open("slk:////arch/bb1203/data.nc",
                          slk_cache="/scratch/b/b12346").open()
        dset = xr.open_dataset(url)
    """

    write_msg: str = "Write mode is not suppored"
    """Error message that is thrown if the files are attempted to be opened
    in any kind of write mode."""

    def __init__(
        self,
        url: str,
        local_file: str,
        slk_cache: Union[str, Path],
        *,
        override: bool = True,
        mode: str = "rb",
        touch: bool = False,
        file_permissions: int = 0o644,
        dir_permissions: int = 0o3775,
        delay: int = 2,
        _lock: threading.Lock = _retrieval_lock,
        _file_queue: Queue[Tuple[str, str]] = FileQueue,
        **kwargs: Any,
    ):
        if not set(mode) & set("r"):  # The mode must have a r
            raise NotImplementedError(self.write_msg)
        if "b" not in mode:
            kwargs.setdefault("encoding", "utf-8")
        self._file = str(Path(local_file).expanduser().absolute())
        self._url = str(url)
        self.slk_cache = Path(slk_cache)
        self.touch = touch
        self.file_permissions = file_permissions
        self.dir_permissions = dir_permissions
        self._order_num = 0
        self._file_obj: Optional[IO[Any]] = None
        self._lock = _lock
        self.kwargs = kwargs
        self.mode = mode
        self.newlines = None
        self.error = "strict"
        self.encoding = kwargs.get("encoding")
        self.write_through = False
        self.delay = delay
        self._file_queue = _file_queue
        #        print(self._file)
        with _lock:
            if not Path(self._file).exists() or override:
                self._file_queue.put((self._url, str(Path(self._file).parent)))
            elif Path(self._file).exists():
                if self.touch:
                    Path(self._file).touch()
                self._file_obj = open(self._file, mode, **kwargs)

    @property
    def name(self) -> str:
        """Get the file for the SLKFile object."""
        if self._file_obj is not None:
            return self._file
        return self._url

    # flake8: noqa: C901
    def _retrieve_items(self, retrieve_files: list[tuple[str, str]]) -> None:
        """Get items from the tape archive.

        Needs to be implemented:

        * check in the beginning whether files are already present in the dst location


        """
        logger.debug("retrieval routine initializing")
        retrieve_files_corrected: list[tuple[str, str]] = list()
        for inp_file, out_dir in retrieve_files:
            self._mkdirs(out_dir)
            # this `mkdir` indirectly sets proper access permissions for this folder
            out_file: str = os.path.join(
                os.path.expanduser(out_dir), Path(inp_file).name
            )
            if os.path.exists(out_file):
                details_inp_file = pyslk.list_clone_file(
                    inp_file, print_timestamps_as_seconds_since_1970=True
                )
                size_out_file = os.path.getsize(out_file)
                mtime_out_file = os.path.getmtime(out_file)
                if (
                    int(details_inp_file.filesize.iloc[0]) == size_out_file
                    and int(details_inp_file.timestamp_mtime.iloc[0]) == mtime_out_file
                ):
                    # do not retrieve file because it exists already in destination and has
                    # same size and timestamp
                    continue
            retrieve_files_corrected.append((inp_file, out_dir))
        # declare variables
        files_retrieval_failed: dict[str, str] = dict()
        # tape_job_mapping: {"<tape_barcode>": [<job_id_1>, <job_id_2>, ...]}
        tape_job_mapping: dict[str, list[int]] = defaultdict(list)
        multi_tape_file_job_mapping: dict[int, list[int]] = defaultdict(list)
        # overview over active jobs
        job_tape_mapping: dict[int, str] = dict()
        job_multi_tape_file_mapping: dict[int, int] = dict()
        # store tapes and multi-tape-files to process later
        file_ids_multiple_tapes: list[int] = list()
        tapes: list[str]
        # all tapes / all multi-tape-files done
        all_tapes_done: bool
        all_multi_tape_files_done: bool
        # list of failed (after MAX_RETRIES_RECALL recalls) and successful recalls
        tapes_success: set[str] = set()
        tapes_active: set[str] = set()
        tapes_failed: dict[str, str] = dict()
        multi_tape_files_success: set[int] = set()
        multi_tape_files_active: set[int] = set()
        multi_tape_files_failed: dict[int, str] = dict()
        # list of files which recalls failed
        files_recall_failed: dict[str, str] = dict()
        # mapping from tape to file list
        tape_file_mapping: dict[str, list[int]] = defaultdict(list)
        # start
        logger.debug(
            "Retrieving %i items from tape (%i already available)",
            len(retrieve_files_corrected),
            len(retrieve_files) - len(retrieve_files_corrected),
        )
        # tape grouping
        file_tape_grouping: list[TapeGroup] = pyslk.group_files_by_tape(
            [inp_file for inp_file, out_dir in retrieve_files_corrected]
        )
        # get list of all tape barcodes (volume_ids)
        tapes = [
            tape_group["barcode"]
            for tape_group in file_tape_grouping
            if tape_group.get("id", -1) > 0
        ]
        all_tapes_done = len(tapes) == 0
        # get list of file split amongst multiple tapes
        for tape_group in file_tape_grouping:
            # check if there are files on multiple tapes included
            if (
                tape_group.get("id", 0) == -1
                and tape_group.get("location", "") == "tape"
            ):
                file_ids_multiple_tapes = tape_group["file_ids"]
            if (
                tape_group.get("location", "") == "tape"
                and tape_group.get("id", -1) > 0
            ):
                tape_file_mapping[tape_group["barcode"]] = tape_group["file_ids"]
        all_multi_tape_files_done = len(file_ids_multiple_tapes) == 0

        # +==========================================================
        # | start _start_recalls()
        # +==========================================================
        def _start_recalls() -> None:
            nonlocal file_ids_multiple_tapes
            nonlocal tapes_success
            nonlocal multi_tape_files_success
            nonlocal tapes_active
            nonlocal multi_tape_files_active
            nonlocal tapes_failed
            nonlocal multi_tape_files_failed
            nonlocal job_tape_mapping
            nonlocal job_multi_tape_file_mapping
            nonlocal tape_job_mapping
            nonlocal multi_tape_file_job_mapping
            nonlocal all_tapes_done
            nonlocal all_multi_tape_files_done
            nonlocal tapes
            nonlocal tape_file_mapping
            file_ids: list[int]
            job_ids_to_be_removed: set[int]
            job_id: int
            logger.debug("Recall function started")
            msg: str
            job_status: pyslk.base.stati.StatusJob

            # +----------------------------------------------------------
            # | CHECK IF WE NEED TO RUN THIS FUNCTION
            # +----------------------------------------------------------
            # all_tapes_done is not set => check if conditions to set this are fulfilled => print user info once
            if (
                len(
                    [
                        tape
                        for tape in tapes
                        if tape not in tapes_success and tape not in tapes_failed
                    ]
                )
                == 0
                and not all_tapes_done
            ):
                all_tapes_done = True
                logger.info("All tapes have been processed.")

            # all_multi_tape_files_done is not set => check if conditions to set this are fulfilled
            #   => print user info once
            if (
                len(
                    [
                        file_id
                        for file_id in file_ids_multiple_tapes
                        if file_id not in multi_tape_files_success
                        and file_id not in multi_tape_files_failed
                    ]
                )
                == 0
                and not all_multi_tape_files_done
            ):
                all_multi_tape_files_done = True
                logger.info(
                    "All files split amongst multiple tapes have been processed."
                )

            # leave directly if all tapes and all multi-tape-files have been processed
            if all_tapes_done and all_multi_tape_files_done:
                logger.debug("Recall function has nothing to do; leaving")
                return

            # +----------------------------------------------------------
            # | CHECK STATUS OF RUNNING JOBS
            # +----------------------------------------------------------
            # check if there are jobs running for whole tapes:
            logger.info(
                "Number of running jobs based on tape: %i", len(job_tape_mapping)
            )
            if not all_tapes_done and len(job_tape_mapping) > 0:
                job_ids_to_be_removed = set()
                # iterate all ids of running jobs
                for job_id, tape_barcode in job_tape_mapping.items():
                    logger.debug(
                        "Checking status of job %i (tape: %s)", job_id, tape_barcode
                    )
                    job_status = pyslk.get_job_status(job_id)
                    # DIFFERENT JOB STATES
                    if job_status.is_successful():
                        # SUCCESS => mark tape as successful; remember job id to be considered as free;
                        # consider this job to be done
                        msg = f"Job {job_id} ended successfully (tape: {tape_barcode})."
                        logger.debug(msg)
                        tapes_active.remove(tape_barcode)
                        tapes_success.add(tape_barcode)
                        job_ids_to_be_removed.add(job_id)
                    elif job_status.is_queued() or job_status.is_processing():
                        # STILL WAITING OR BEING PROCESSED => do nothing; just wait further
                        msg = f"Job {job_id} not finished yet (tape: {tape_barcode})."
                        logger.debug(msg)
                        pass
                    elif job_status.is_paused():
                        # PAUSED => something is wrong => admins manually paused job BUT keep this job as it is
                        # log warning message; but do nothing else
                        msg = f"Job {job_id} in paused state (tape: {tape_barcode}). Waiting for this job."
                        logger.warning(msg)
                    elif job_status.has_failed():
                        # FAILED => something went wrong => can be restarted
                        #   DO NOT RESTART if MAX_RETRIES_RECALL of retries have been reached
                        # log warning message; but do nothing else
                        msg = (
                            f"Job {job_id} has failed (tape: {tape_barcode}). "
                            + f"{4 - len(tape_job_mapping.get(tape_barcode, list()))} of "
                            + f"{MAX_RETRIES_RECALL} retries left"
                        )
                        logger.warning(msg)
                        tapes_active.remove(tape_barcode)
                        job_ids_to_be_removed.add(job_id)
                        if (
                            len(tape_job_mapping.get(tape_barcode, list()))
                            >= MAX_RETRIES_RECALL + 1
                        ):
                            # consider this tape to fail permanently
                            msg = (
                                "max retries reached (jobs failed: "
                                + f"{', '.join([str(job_id) for job_id in tape_job_mapping[tape_barcode]])})"
                            )
                            tapes_failed[tape_barcode] = msg
                            logger.error(msg)
                            # get file ids
                            file_ids = tape_file_mapping[tape_barcode]
                            for file_id in file_ids:
                                files_recall_failed[pyslk.resource_path(file_id)] = msg
                    else:
                        # SOMETHING ELSE ...
                        # unexpected state; log warning message; but do nothing else
                        msg = (
                            f"Job {job_id} has unexpected state (tape id: {tape_barcode}): "
                            + f"{job_status.get_status_name()}. Do not proceed with this tape."
                        )
                        logger.error(msg)
                        job_ids_to_be_removed.add(job_id)
                        tapes_active.remove(tape_barcode)
                        tapes_failed[tape_barcode] = msg
                        # get file ids
                        file_ids = tape_file_mapping[tape_barcode]
                        for file_id in file_ids:
                            files_recall_failed[pyslk.resource_path(file_id)] = msg
                # remove ids of jobs which ended
                for job_id_to_be_removed in job_ids_to_be_removed:
                    del job_tape_mapping[job_id_to_be_removed]

            logger.info(
                "Number of running jobs based on split files: %i",
                len(job_multi_tape_file_mapping),
            )
            # check if there are jobs running for files split amongst multiple tapes:
            if not all_multi_tape_files_done and len(job_multi_tape_file_mapping) > 0:
                job_ids_to_be_removed = set()
                # iterate all ids of running jobs
                for job_id, file_id in job_multi_tape_file_mapping.items():
                    logger.debug(
                        "Checking status of job %i (file id: %i)", job_id, file_id
                    )
                    job_status = pyslk.get_job_status(job_id)
                    # DIFFERENT JOB STATES
                    if job_status.is_successful():
                        # SUCCESS => mark tape as successful; remember job id to be considered as free;
                        # consider this job to be done
                        msg = f"Job {job_id} ended successfully (file id: {file_id})."
                        logger.debug(msg)
                        multi_tape_files_success.add(file_id)
                        job_ids_to_be_removed.add(job_id)
                    elif job_status.is_queue() or job_status.is_processing():
                        # STILL WAITING OR BEING PROCESSED => do nothing; just wait further
                        msg = f"Job {job_id} not finished yet (file id: {file_id})."
                        logger.debug(msg)
                        pass
                    elif job_status.is_paused():
                        # PAUSED => something is wrong => admins manually paused job BUT keep this job as it is
                        # log warning message; but do nothing else
                        msg = f"Job {job_id} in paused state (file id: {file_id}). Waiting for this job."
                        logger.warning(msg)
                    elif job_status.has_failed():
                        # FAILED => something went wrong => can be restarted
                        #   DO NOT RESTART if MAX_RETRIES_RECALL of retries have been reached
                        # log warning message; but do nothing else
                        msg = (
                            f"Job {job_id} has failed (file id: {file_id}). "
                            + f"{4 - len(multi_tape_file_job_mapping.get(file_id, list()))} of "
                            + f"{MAX_RETRIES_RECALL} retries left"
                        )
                        logger.warning(msg)
                        job_ids_to_be_removed.add(job_id)
                        if (
                            len(multi_tape_file_job_mapping.get(file_id, list()))
                            >= MAX_RETRIES_RECALL + 1
                        ):
                            # consider this job to be done
                            msg = (
                                "max retries reached (jobs failed: "
                                + f"{', '.join([str(job_id) for job_id in multi_tape_file_job_mapping[file_id]])})"
                            )
                            logger.error(msg)
                            multi_tape_files_failed[file_id] = msg
                            files_recall_failed[pyslk.resource_path(file_id)] = msg
                    else:
                        # SOMETHING ELSE ...
                        # unexpected state; log warning message; but do nothing else
                        msg = (
                            f"Job {job_id} has unexpected state (file id: {file_id}): "
                            + f"{job_status.get_status_name()}. Do not proceed with this tape."
                        )
                        logger.error(msg)
                        job_ids_to_be_removed.add(job_id)
                        multi_tape_files_failed[file_id] = msg
                        files_recall_failed[pyslk.resource_path(file_id)] = msg
                # remove ids of jobs which ended
                for job_id_to_be_removed in job_ids_to_be_removed:
                    del job_multi_tape_file_mapping[job_id_to_be_removed]

            # +----------------------------------------------------------
            # | SUBMIT NEW JOBS IF NECESSARY
            # +----------------------------------------------------------
            # start new recalls if there are less than the max number of these jobs are running and tapes are free
            tapes_available = [
                tape
                for tape in tapes
                if (
                    tape not in tapes_success
                    and tape not in tapes_failed
                    and tape not in tapes_active
                )
            ]
            logger.debug(
                "Number of tapes for which recalls need to be submitted: %i",
                len(tapes_available),
            )
            logger.debug(
                "Number of running jobs: %i",
                len(job_tape_mapping) + len(job_multi_tape_file_mapping),
            )
            logger.debug("Maximum allowed number of jobs: %i", MAX_PARALLEL_RECALLS)
            if (
                len(tapes_available) > 0
                and len(job_tape_mapping) + len(job_multi_tape_file_mapping)
                < MAX_PARALLEL_RECALLS
            ):
                # iterate over all tapes until
                #  (a) all tapes were iterated or
                #  (b) the maximum number of parallel recalls has been reached
                for tape in tapes_available:
                    if (
                        len(job_tape_mapping) + len(job_multi_tape_file_mapping)
                        > MAX_PARALLEL_RECALLS
                    ):
                        logger.debug(
                            "Submitting no additional recalls because max number of parallel recalls has been reached."
                        )
                        break
                    logger.debug("Considering tape %s for next recall.", tape)
                    # go through tape list and start new recalls
                    tape_status = pyslk.get_tape_status(tape)
                    logger.debug("Tape %s status: %s", tape, tape_status)
                    if tape_status == "BLOCKED":
                        # do nothing
                        msg = f"Tape {tape} is blocked. Skip it until next time."
                        logger.debug(msg)
                    elif tape_status == "FAILED":
                        msg = f"Tape {tape} is in failed state. Do not proceed getting data from this tape."
                        logger.error(msg)
                        tapes_failed[tape] = msg
                        # get file ids
                        file_ids = tape_file_mapping[tape]
                        for file_id in file_ids:
                            files_recall_failed[pyslk.resource_path(file_id)] = msg
                    elif tape_status == "AVAILABLE":
                        # start new job
                        msg = f"Tape {tape} is available. Starting recall from tape."
                        logger.debug(msg)
                        # get file ids
                        file_ids = tape_file_mapping[tape]
                        # really start new job here
                        job_id = pyslk.recall_single(file_ids, resource_ids=True)
                        logger.info(
                            f"Recall job started for tape {tape}: {str(job_id)}"
                        )
                        # bijective job id <-> tape
                        job_tape_mapping[job_id] = tape
                        # tape -> multiple job idss
                        tape_job_mapping[tape].append(job_id)
                        # just the active tapes
                        tapes_active.add(tape)
                    else:
                        # unexpected state; log warning message; but do nothing else
                        msg = f"Tape {tape} has unexpected state: {tape_status}. Do not proceed with this tape."
                        logger.error(msg)
                        tapes_failed[tape] = msg
                        # get file ids
                        file_ids = tape_file_mapping[tape]
                        for file_id in file_ids:
                            files_recall_failed[pyslk.resource_path(file_id)] = msg

            # iterate over files stored on multiple tapes each
            multi_tape_files_available = [
                file_id
                for file_id in file_ids_multiple_tapes
                if (
                    file_id not in multi_tape_files_success
                    and file_id not in multi_tape_files_failed
                    and file_id not in multi_tape_files_active
                )
            ]
            logger.debug(
                "Number of files for which recalls need to be submitted: %i",
                len(multi_tape_files_available),
            )
            logger.debug(
                "Number of running jobs: %i",
                len(job_tape_mapping) + len(job_multi_tape_file_mapping),
            )
            logger.debug("Maximum allowed number of jobs: %i", MAX_PARALLEL_RECALLS)
            if (
                len(multi_tape_files_available) > 0
                and len(job_tape_mapping) + len(job_multi_tape_file_mapping)
                < MAX_PARALLEL_RECALLS
            ):

                tmp_tapes_available: List[bool]
                tmp_tapes: List[str]
                # for loop over file ids
                for file_id in multi_tape_files_available:
                    if (
                        len(job_tape_mapping) + len(job_multi_tape_file_mapping)
                        > MAX_PARALLEL_RECALLS
                    ):
                        logger.debug(
                            "Submitting no additional recalls because max number of parallel recalls has been reached."
                        )
                        break
                    logger.debug("Considering file %i for next recall.", file_id)
                    # TEST files:
                    # * /arch/pd1309/forcings/reanalyses/ERA5/year2009/ERA5_2009_09_part5.tar
                    tmp_tapes = list()
                    tmp_tapes_available = list()
                    for tape_id, tape_barcode in pyslk.get_resource_tapes(
                        pyslk.get_resource_path(file_id)
                    ):
                        tmp_tapes.append(tape_barcode)
                    if len(tmp_tapes) < 2:
                        msg = (
                            f"File {file_id} is in list of split files but it seems to be stored on "
                            + f"{len(tmp_tapes)} tape."
                        )
                        logger.error(msg)
                        raise pyslk.PySlkException(msg)
                    logger.debug(f"File {file_id} on tapes: {', '.join(tmp_tapes)}")
                    for tape in tmp_tapes:
                        # go through tape list and start new recalls
                        tape_status = pyslk.get_tape_status(tape)
                        if tape_status == "BLOCKED":
                            # do nothing
                            msg = f"Tape {tape} is blocked (file {file_id}). Skip split file until next time."
                            logger.debug(msg)
                            tmp_tapes_available.append(False)
                        elif tape_status == "FAILED":
                            msg = (
                                f"Tape {tape} is in failed state (file {file_id}). Do not proceed getting "
                                + "split file."
                            )
                            logger.error(msg)
                            multi_tape_files_failed[file_id] = msg
                            files_recall_failed[pyslk.resource_path(file_id)] = msg
                            tmp_tapes_available.append(False)
                        elif tape_status == "AVAILABLE":
                            # start new job
                            msg = f"Tape {tape} is available (file {file_id})."
                            logger.debug(msg)
                            tmp_tapes_available.append(True)
                        else:
                            # unexpected state; log warning message; but do nothing else
                            msg = (
                                f"Tape {tape} has unexpected state (file {file_id}): {tape_status}. Do not "
                                + "proceed with this tape."
                            )
                            logger.error(msg)
                            multi_tape_files_failed[file_id] = msg
                            files_recall_failed[pyslk.resource_path(file_id)] = msg
                            tmp_tapes_available.append(False)
                    if all(tmp_tapes_available):
                        # really start new job here
                        job_id = pyslk.recall_single(file_id, resource_ids=True)
                        # bijective job id <-> tape
                        job_multi_tape_file_mapping[job_id] = file_id
                        # tape -> multiple job ids
                        multi_tape_file_job_mapping[file_id].append(job_id)
                        # just the active tapes
                        multi_tape_files_active.add(file_id)

            logger.debug("Recall function ended")

        # +==========================================================
        # | end _start_recalls()
        # +==========================================================

        to_be_retrieved_files: set[str] = set(
            [inp_file for inp_file, out_dir in retrieve_files_corrected]
        )
        iterations: int = 0
        # +----------------------------------------------------------
        # | iterate as long as there are files to retrieve
        # |  after each retrieval, start recalls
        # +----------------------------------------------------------
        _start_recalls()
        recall_timer = time.time()
        # we do not generally remove files_recall_failed from to_be_retrieved because some files of failed recalls
        # might have been recalled
        while (
            len(
                [
                    file_path
                    for file_path in to_be_retrieved_files
                    if file_path not in files_recall_failed.keys()
                ]
            )
            > 0
        ):
            iterations += 1
            retrieve_timer = time.time()
            logger.info(
                "retrieve/recall iteration %i; %i files to be retrieved. %i recall jobs running for %i files.",
                iterations,
                len(to_be_retrieved_files),
                len(job_tape_mapping) + len(job_multi_tape_file_mapping),
                sum(
                    [len(tape_file_mapping[tape]) for tape in job_tape_mapping.values()]
                )
                + len(job_multi_tape_file_mapping),
            )
            for inp_file, out_dir in retrieve_files_corrected:
                # skip files which do not need to be retrieved anymore
                if inp_file not in to_be_retrieved_files:
                    continue
                Path(out_dir).mkdir(
                    parents=True, exist_ok=True, mode=self.file_permissions
                )
                # check if recalls need to be started before retrieving
                # check every 5 minutes whether additional recalls need to be started
                if time.time() - recall_timer > 300:
                    _start_recalls()
                    recall_timer = time.time()
                # check if file should be retrieved or not
                output_dry_retrieve = pyslk.retrieve_improved(
                    inp_file, out_dir, dry_run=True, preserve_path=False
                )
                # example output:
                """
                {'SKIPPED': {'SKIPPED_TARGET_EXISTS': ['/arch/bm0146/k204221/iow/INDEX.txt']},
                    'FILES': {'/arch/bm0146/k204221/iow/INDEX.txt': '/home/k204221/tmp/INDEX.txt'}}

                # dry run
                {'ENVISAGED': {'ENVISAGED': ['/arch/bm0146/k204221/iow/INDEX.txt']},
                    'FILES': {'/arch/bm0146/k204221/iow/INDEX.txt': '/home/k204221/tmp/abcdef2/INDEX.txt'}}

                # after successful retrieval
                {'ENVISAGED': {'ENVISAGED': []}, 'FILES': {'/arch/bm0146/k204221/iow/INDEX.txt':
                    '/home/k204221/tmp/INDEX.txt'}, 'SUCCESS': {'SUCCESS': ['/arch/bm0146/k204221/iow/INDEX.txt']}}

                {'FAILED': {'FAILED_NOT_CACHED': ['/arch/bm0146/k204221/iow/iow_data5_001.tar']},
                    'FILES': {'/arch/bm0146/k204221/iow/iow_data5_001.tar': '/home/k204221/tmp/iow_data5_001.tar'}}
                """
                # check if file should be skipped
                if "SKIPPED" in output_dry_retrieve:
                    logger.debug(
                        f"File {inp_file} does already exist in {out_dir}. Skip."
                    )
                    to_be_retrieved_files.remove(inp_file)
                    continue
                # check if file somehow cannot be retrieved
                if "FAILED" in output_dry_retrieve:
                    if "FAILED_NOT_CACHED" in output_dry_retrieve["FAILED"]:
                        logger.debug(f"File {inp_file} is not cached yet. Retry later.")
                        continue
                    else:
                        logger.error(
                            f"File {inp_file} cannot be retrieved for unknown reasons. Ignore."
                        )
                        to_be_retrieved_files.remove(inp_file)
                        files_retrieval_failed[inp_file] = next(
                            iter(output_dry_retrieve["FAILED"])
                        )
                        continue
                # check if file should be retrieved
                if "ENVISAGED" in output_dry_retrieve:
                    # message on which file is retrieved to where
                    logger.debug(f"Retrieving file {inp_file} to {out_dir}")
                    # new retrieve command
                    output_retrieve = pyslk.retrieve_improved(
                        inp_file, out_dir, dry_run=False, preserve_path=False
                    )
                    # check if file somehow could not be retrieved
                    if "FAILED" in output_retrieve:
                        if "FAILED_NOT_CACHED" in output_retrieve["FAILED"]:
                            logger.debug(
                                f"File {inp_file} could not be retrieve because itis not cached. Retry later."
                            )
                            continue
                        else:
                            logger.error(
                                f"File {inp_file} could not be retrieved for unknown reasons. Ignore."
                            )
                            to_be_retrieved_files.remove(inp_file)
                            files_retrieval_failed[inp_file] = next(
                                iter(output_retrieve["FAILED"])
                            )
                            continue
                    # check if file was skipped
                    if "SKIPPED" in output_retrieve:
                        logger.debug(
                            f"File {inp_file} was not retrieve because it does already exist in {out_dir}. Skip."
                        )
                        to_be_retrieved_files.remove(inp_file)
                        continue
                    # check if file was successfully retrieved
                    if "SUCCESS" in output_retrieve:
                        logger.debug(
                            f"File {inp_file} was successfully retrieved to {out_dir}."
                        )
                        logger.debug("Adjusting file permissions")
                        Path(
                            os.path.join(
                                os.path.expanduser(out_dir), Path(inp_file).name
                            )
                        ).chmod(self.file_permissions)
                        to_be_retrieved_files.remove(inp_file)
                        continue
                logger.error(
                    f"Retrieval check for file {inp_file} yielded unexpected output. Ignore."
                )
                files_retrieval_failed[inp_file] = (
                    f"unexpected JSON output of pyslk.retrieve_improved: {json.dumps(output_dry_retrieve)}"
                )
                to_be_retrieved_files.remove(inp_file)
            if len(to_be_retrieved_files) > 0 and time.time() - retrieve_timer < 60:
                logger.info(
                    f"Waiting for {60 - (time.time() - retrieve_timer)} seconds before next retrieval."
                )
                time.sleep(60 - (time.time() - retrieve_timer))

        # +----------------------------------------------------------
        # | print files which are not available
        # +----------------------------------------------------------
        missing_files: list[str] = [
            file_path
            for file_path in to_be_retrieved_files
            if file_path not in files_recall_failed.keys()
            and file_path not in files_retrieval_failed.keys()
        ]
        tmp_str: str
        if (
            len(to_be_retrieved_files) > 0
            or len(files_recall_failed.keys()) > 0
            or len(files_retrieval_failed.keys()) > 0
        ):
            timestamp: str = datetime.now().strftime("%Y%m%dT%H%M%S")
            file_failed_base: str = f"files_failed_{timestamp}"
            file_failed_recall: str = f"{file_failed_base}_recall.txt"
            file_failed_retrieve: str = f"{file_failed_base}_retrieve.txt"
            file_failed_other: str = f"{file_failed_base}_other.txt"
            logger.error(
                "One or more files could not be retrieved from the tape archive. They "
                + f"are printed below and written into files '{file_failed_base}_*.txt'"
                + f"in directory '{str(self.slk_cache)}'."
            )
            if len(files_recall_failed) > 0:
                tmp_str = "\n  ".join(files_recall_failed)
                logger.error(f"files, recall failed:\n  {tmp_str}")
                with open(os.path.join(self.slk_cache, file_failed_recall), "w") as f:
                    for file_path, reason in files_recall_failed.items():
                        f.write(f"{file_path}: {reason}\n")
            if len(files_retrieval_failed) > 0:
                tmp_str = "\n  ".join(files_retrieval_failed)
                logger.error(
                    f"files, retrieval failed (recall successful):\n  {tmp_str}"
                )
                with open(os.path.join(self.slk_cache, file_failed_retrieve), "w") as f:
                    for file_path, reason in files_retrieval_failed.items():
                        f.write(f"{file_path}: {reason}\n")
            if len(missing_files) > 0:
                tmp_str = "\n  ".join(missing_files)
                logger.error(f"files, missing for other reasons:\n  {tmp_str}")
                with open(os.path.join(self.slk_cache, file_failed_other), "w") as f:
                    for file_path in missing_files:
                        f.write(f"{file_path}: failed for unknown reasons\n")

    def _cache_files(self) -> None:
        time.sleep(self.delay)
        with self._lock:
            items = []
            if self._file_queue.qsize() > 0:
                self._file_queue.put(("finish", "finish"))
                for _ in range(self._file_queue.qsize() - 1):
                    items.append(self._file_queue.get())
                    self._file_queue.task_done()
                try:
                    self._retrieve_items(items)
                except Exception as error:
                    _ = [
                        self._file_queue.get() for _ in range(self._file_queue.qsize())
                    ]
                    self._file_queue.task_done()
                    raise error
                _ = self._file_queue.get()
                self._file_queue.task_done()
        self._file_queue.join()
        self._file_obj = open(self._file, self.mode, **self.kwargs)

    def __fspath__(self) -> str:
        if self._file_obj is None:
            self._cache_files()
        return self.name

    def tell(self) -> int:
        if self._file_obj is None:
            self._cache_files()
        return self._file_obj.tell()  # type: ignore

    def seek(self, target: int) -> int:  # type: ignore
        if self._file_obj is None:
            self._cache_files()
        return self._file_obj.seek(target)  # type: ignore

    def _mkdirs(self, path: Union[str, Path]) -> None:
        rp = os.path.realpath(path)
        if os.access(rp, os.F_OK):
            if not os.access(rp, os.W_OK):
                raise PermissionError(
                    f"Cannot write to directory, {rp}, needed for downloading data. Probably, you lack access privileges."
                )
            return
        components = Path(rp).parts[1:]
        for i in range(len(components)):
            subpath = Path("/", *components[: i + 1])
            if not os.access(subpath, os.F_OK):
                try:
                    os.mkdir(subpath)
                except PermissionError as e:
                    raise PermissionError(
                        f"Cannot create or access directory, {e.filename}, needed for downloading data."
                    )
                os.chmod(subpath, self.dir_permissions)

    @staticmethod
    def readable() -> Literal[True]:
        """Compatibility method."""
        return True

    @staticmethod
    def writeable() -> Literal[False]:
        """Compatibility method."""
        return False

    @staticmethod
    def seekable() -> Literal[True]:
        """Compatibility method."""
        return True

    def read(self, size: int = -1) -> Any:
        """The the content of a file-stream.

        size: int, default: -1
            read at most size characters from the stream, -1 means everything
            is read.
        """
        if self._file_obj is None:
            self._cache_files()
        return self._file_obj.read(size)  # type: ignore

    @staticmethod
    def flush() -> None:
        """Flushing file systems shouldn't work for ro modes."""
        return None

    def writelines(self, *arg: Any) -> None:
        """Compatibility method."""
        raise NotImplementedError(self.write_msg)

    def write(self, *arg: Any) -> None:
        """Writing to tape is not supported."""
        raise NotImplementedError(self.write_msg)

    def close(self) -> None:
        if self._file_obj is not None:
            self._file_obj.close()


class SLKFileSystem(AbstractFileSystem):
    """Abstract class for hsm files systems.

    The implementation intracts with the hsm tape storage system, files
    that are accessed are downloaded to a temporary data storage.

    Parameters
    ----------

    slk_cache: str | Path, default: None
        Destination of the temporary storage. This directory is used to
        retrieve data from tape.
    block_size: int, default: None
         Some indication of buffering - this is a value in bytes
    file_permissions: int, default: 0o644
        Permission when creating files.
    dir_permissions: int, default: 0o3775
        Permission when creating directories.

    override: bool, default: False
        Override existing files
    touch: bool, default: False
        Update existing files on the temporary storage to prevent them
        from being deleted. // not necessary if they are read.
    **storage_options:
        Additional options passed to the AbstractFileSystem class.
    """

    protocol = "slk"
    local_file = True
    sep = "/"

    def __init__(
        self,
        block_size: Optional[int] = None,
        slk_cache: Optional[Union[str, Path]] = None,
        file_permissions: int = 0o644,
        dir_permissions: int = 0o3775,
        touch: bool = False,
        delay: int = 2,
        override: bool = False,
        **storage_options: Any,
    ):
        super().__init__(
            block_size=block_size,
            asynchronous=False,
            loop=None,
            **storage_options,
        )
        slk_options = storage_options.get("slk", {})
        slk_cache = (
            slk_options.get("slk_cache", None)
            or slk_cache
            or os.environ.get("SLK_CACHE")
        )
        if not slk_cache:
            slk_cache = f"/scratch/{getuser()[0]}/{getuser()}"
            warnings.warn(
                "Neither the slk_cache argument nor the SLK_CACHE environment "
                "variable is set. Falling back to default "
                f"{slk_cache}",
                UserWarning,
                stacklevel=2,
            )
        self.touch = touch
        self.slk_cache = Path(slk_cache)
        self.override = override
        self.delay = delay
        self.file_permissions = file_permissions
        self.dir_permissions = dir_permissions

    @overload
    def ls(
        self, path: Union[str, Path], detail: Literal[True], **kwargs: Any
    ) -> List[FileInfo]: ...

    @overload
    def ls(
        self, path: Union[str, Path], detail: Literal[False], **kwargs: Any
    ) -> List[str]: ...

    def ls(
        self, path: Union[str, Path], detail: bool = True, **kwargs: Any
    ) -> Union[List[FileInfo], List[str]]:
        """List objects at path.

        This includes sub directories and files at that location.

        Parameters
        ----------
        path: str | pathlib.Path
            Path of the file object that is listed.
        detail: bool, default: True
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).


        Returns
        -------
        list : List of strings if detail is False, or list of directory
               information dicts if detail is True.
        """
        path = Path(path)
        filelist: pd.DataFrame = pyslk.ls(str(path), full_path=True)
        detail_list: List[FileInfo] = []
        types = {"d": "directory", "-": "file"}
        for index, row in filelist.iterrows():
            entry: FileInfo = {
                "name": str(row.filename),
                "size": int(row.filesize),
                "type": types[row.permissions[0]],
            }
            detail_list.append(entry)
        if detail:
            return detail_list
        return filelist.filename.tolist()

    def _open(
        self,
        path: str | Path,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SLKFile:
        path = Path(self._strip_protocol(path))
        local_path = self.slk_cache.joinpath(*path.parts[1:])
        return SLKFile(
            str(path),
            str(local_path),
            self.slk_cache,
            mode=mode,
            override=self.override,
            touch=self.touch,
            delay=self.delay,
            encoding=kwargs.get("encoding"),
            file_permissions=self.file_permissions,
            dir_permissions=self.dir_permissions,
        )
