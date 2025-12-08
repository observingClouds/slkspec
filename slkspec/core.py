from __future__ import annotations

import io
import json
import logging
import os
import random
import string
import threading
import time
import warnings
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

import hsm
import pandas as pd
import pyslk
from fsspec.spec import AbstractFileSystem

logger = logging.getLogger("slkspec")
if logger.level == 0:
    if logging.root.level == 0:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.root.level)

# default delay for the queue to collect resources: 5 seconds
default_queue_delay = 5
# define queue
FileQueue: Queue[Tuple[str, str]] = Queue(maxsize=-1)
# other definitions
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
        Permission when creating directories.
    **kwargs:
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
        delay: int = default_queue_delay,
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

    def url(self) -> str:
        """HTTP URL to read this file (if it already exists)"""
        # return self.fs.url(self.path, **kwargs)
        return self._url

    def _retrieve_items(self, retrieve_files: set[tuple[str, str]]) -> None:
        """Get items from the tape archive.
        Retrieves items using given list of files and performs necessary operations.

        Parameters:
        - retrieve_files: a set of tuples containing file source and destination

        Returns: None
        """
        logger.debug("retrieval routine initializing")
        retrieve_files_corrected: set[tuple[str, str]] = _reformat_retrieve_files_list(
            retrieve_files=retrieve_files,
            dir_permissions=self.dir_permissions,
        )
        # start
        logger.debug(
            "Planning to retrieve %i items from cache/tape",
            len(retrieve_files_corrected),
        )
        # instantiate recall and retrieval classes
        hsm_proxy: HSMProxy = HSMProxy(
            {inp_file for inp_file, out_dir in retrieve_files_corrected}
        )
        slk_retrieval: SLKRetrieval = SLKRetrieval(
            hsm_proxy,
            retrieve_files_corrected,
            self.file_permissions,
        )

        # iterate as long as there are files to retrieve; but first start recalls
        #  (after each retrieval, start recalls; done in retrieval class)
        iterations: int = 0
        retrieve_timer: float
        # we do not generally remove files_recall_failed from to_be_retrieved because
        # some files of failed recalls might have been recalled
        while slk_retrieval.number_files_still_to_be_retrieved_realistically() > 0:
            iterations += 1
            retrieve_timer = time.time()
            logger.info(
                (
                    "retrieve/recall iteration %i; %i files requested; of "
                    + "which %i failed and %i succeeded"
                ),
                iterations,
                len(slk_retrieval.files_retrieval_requested),
                len(slk_retrieval.files_retrieval_failed),
                len(slk_retrieval.files_retrieval_succeeded),
            )
            slk_retrieval.run_retrieval()
            if slk_retrieval.number_files_still_to_be_retrieved_realistically() > 0:
                # if
                #  * still files need to be retrieved AND
                #  * recall job is done
                # then submit a new recall job
                if not hsm_proxy.job_active:
                    # new job
                    hsm_proxy_new: HSMProxy = HSMProxy(
                        slk_retrieval.get_files_still_to_be_retrieved_realistically(),
                        hsm_proxy.files_broken,
                    )
                    # in retrieval object: overwrite old by new one
                    slk_retrieval.hsm_proxy = hsm_proxy_new
                    # here: overwrite old by new as well
                    hsm_proxy = hsm_proxy_new
                # wait some time until doing the new retrieval
                # => sometimes we just need to wait for the recalls to be finished
                if time.time() - retrieve_timer < 60:
                    logger.info(
                        f"Waiting for {int(60 - (time.time() - retrieve_timer))} "
                        + "seconds before next retrieval."
                    )
                    time.sleep(60 - (time.time() - retrieve_timer))

        # print files which are not available
        _write_file_lists(hsm_proxy, slk_retrieval, self.slk_cache)

        # throw error if not all files were retrieved
        tmp_sum = len(slk_retrieval.get_files_not_retrieved_due_to_issues())
        if tmp_sum > 0:
            raise pyslk.PySlkException(
                f"{tmp_sum} of requested"
                + f"{len(slk_retrieval.retrieve_files_corrected)} files could not be "
                + "retrieved. Please check previous error messages for affected files."
            )

    def _cache_files(self) -> None:
        time.sleep(self.delay)
        with self._lock:
            items: set = set()
            if self._file_queue.qsize() > 0:
                self._file_queue.put(("finish", "finish"))
                for _ in range(self._file_queue.qsize() - 1):
                    items.add(self._file_queue.get())
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
        """The content of a file-stream.

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
        Update `mtime` of temporary files to prevent them from being deleted. Depending
        on the implemented method to delete temporary files this might not be necessary
        or have no effect.
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
        delay: int = default_queue_delay,
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


class HSMProxy:
    def __init__(
        self,
        files_requested: set[str],
        files_broken: set[str] = set(),
        job_id: str = "",
    ):
        logger.debug("initializing HSMProxy class")
        if not job_id:
            logger.debug("received no job id")
        else:
            logger.debug(f"got job id '{job_id}'")
        # set file list
        self.files_requested: set[str] = files_requested
        # declare a few other variables
        self.files_broken: set[str] = files_broken
        self.files_recall_active: set[str] = set()
        self.files_recall_done: set[str] = set()
        self.job_active: bool = False
        job_submission: hsm.models.JobSubmission
        # start new recall job or link to existing recall job
        if not job_id:
            logger.debug("attempting to start new recall job")
            # submit new recall job
            try:
                # job might exist already => catch this error
                job_submission = hsm.recall(list(files_requested - files_broken))
            except hsm.errors.HSMConflictError as e:
                logger.debug(f"Catched 'hsm.errors.HSMConflictError': {str(e)}")
                # guess job id if job already exists
                job_submission = hsm.recall(
                    list(
                        files_requested - files_broken
                        | {
                            "dummy_file_"
                            + "".join(
                                random.choices(
                                    string.ascii_uppercase + string.digits, k=6
                                )
                            )
                        }
                    )
                )

            if "job_id" not in dir(job_submission):
                # ERROR!
                logger.debug("no job id returned")
                if "message" in dir(job_submission):
                    raise RuntimeError(
                        "Could not submit recall job to HSM proxy with message: "
                        + f"{job_submission.message}"
                    )
                else:
                    raise RuntimeError(
                        "Could not submit recall job to HSM proxy due to unknown "
                        + "error. Please contact support@dkrz.de."
                    )
            self.job_id = job_submission.job_id
            logger.debug(f"got job id '{self.job_id}'")
        else:
            # check if provided job_id actually exists
            try:
                hsm.status(job_id)
            except hsm.errors.HSMNotFoundError:
                raise RuntimeError(f"Provided job id does not exist: '${job_id}'")
            # store job id
            self.job_id = job_id
        # give the job 10 seconds time to initializels
        time.sleep(10)
        # get job status for the first time
        self.update_from_proxy()

    def _extract_file_lists_from_job_status(
        self, job_status: hsm.models.JobStatus
    ) -> None:
        tape_infos: list[dict] = job_status.raw["tape_info"]
        """
        [
        {
            "tape_id": 75691,
            "tape_format": "HPSS",
            "tape_type": "LTO6",
            "tape_library": "StorageTek-SL8500-516000300251.1",
            "files": [
            {
                "source_path": "/arch/bm0146/k204221/iow/iow_data2_004.tar",
                "base_path": "/arch/bm0146/k204221/iow/iow_data2_004.tar",
                "file_id": 49058705501
            }
            ],
            "status": {
            "value": 401,
            "name": "RECALLING_IN_PROGRESS"
            }
        },
        {
            "tape_id": -1,
            "tape_format": "",
            "tape_type": "",
            "tape_library": "",
            "files": [
            {
                "source_path": "/arch/bm0146/k204221/iow/iow_data2_003.tar",
                "base_path": "/arch/bm0146/k204221/iow/iow_data2_003.tar",
                "file_id": 49058705500
            }
            ],
            "status": {
            "value": 402,
            "name": "RECALLING_FINISHED"
            }
        }
        ]
        """
        # go through each entry of the tape infos (this is ether the conent
        #  of on tape or of the cache)
        for tape_info in tape_infos:
            if tape_info["status"]["name"] == "RECALLING_FINISHED":
                # file ist cached
                self.files_recall_done.update(
                    {f["source_path"] for f in tape_info["files"]}
                )
            elif tape_info["status"]["name"] == "RECALLING_IN_PROGRESS":
                # file ist cached
                self.files_recall_active.update(
                    {f["source_path"] for f in tape_info["files"]}
                )
            else:
                raise RuntimeError(
                    "Unexpected status appeared when recall status was checked. "
                    + f"Status: {tape_info['status']['name']}, HTTP Code: "
                    + f"{tape_info['status']['value']}"
                )

    def update_from_proxy(self) -> None:
        # get job status
        this_job_status: hsm.models.JobStatus = hsm.status(self.job_id)
        try:
            logger.debug(f"updating job status (id: '{self.job_id}')")
            logger.debug(f"  job status: '{this_job_status.status_name}'")
            self.job_active = this_job_status.status_name == "RECALLING_IN_PROGRESS"
            if this_job_status.broken_files is None:
                self.files_broken = set()
            else:
                self.files_broken = set(this_job_status.broken_files)
            self._extract_file_lists_from_job_status(this_job_status)
            logger.debug(f"  #files_recall_active: '{len(self.files_recall_active)}'")
            logger.debug(f"  #files_broken: '{len(self.files_broken)}'")
            logger.debug(f"  #files_recall_done: '{len(self.files_recall_done)}'")
        except TypeError as e:
            logger.error(
                "Command 'hsm.status' returned unexpected output which caused "
                + f"TypeError: {str(e)}"
            )

    def check_file_broken(self, file_path: str) -> bool:
        return file_path in self.files_broken

    def check_file_recall_active(self, file_path: str) -> bool:
        return file_path in self.files_recall_active

    def check_file_recall_done(self, file_path: str) -> bool:
        return file_path in self.files_recall_done


class SLKRetrieval:

    def __init__(
        self,
        hsm_proxy: HSMProxy,
        retrieve_files_corrected: set[tuple[str, str]],
        file_permissions: int,
    ) -> None:
        self.hsm_proxy: HSMProxy = hsm_proxy
        self.retrieve_files_corrected: set[tuple[str, str]] = retrieve_files_corrected
        # self.files_retrieval_reasonable: set[str] = set(
        #     [inp_file for inp_file, out_dir in self.retrieve_files_corrected]
        # )
        self.files_retrieval_destination: dict[str, str] = {
            inp_file: out_dir for inp_file, out_dir in self.retrieve_files_corrected
        }
        self.files_retrieval_requested: set[str] = {
            inp_file for inp_file, out_dir in self.retrieve_files_corrected
        }
        self.files_retrieval_failed: dict[str, str] = dict()
        self.files_retrieval_succeeded: set[str] = set()
        self.file_permissions: int = file_permissions
        self.files_unexpectedly_not_cached: set[str] = set()

    def number_files_still_to_be_retrieved_in_total(self) -> int:
        return len(self.files_retrieval_requested)

    def get_files_still_to_be_retrieved_realistically(self) -> set[str]:
        return (
            self.files_retrieval_requested
            - {f for f in self.files_retrieval_failed.keys()}
            - self.files_retrieval_succeeded
            - self.hsm_proxy.files_broken
        )

    def get_files_not_retrieved_due_to_issues(self) -> set[str]:
        return (
            {f for f in self.files_retrieval_failed}
            |
            # use only files which were actually requested
            # (the recall job might target more files!)
            self.hsm_proxy.files_broken.intersection(self.files_retrieval_requested)
        )

    def number_files_still_to_be_retrieved_realistically(self) -> int:
        return len(self.get_files_still_to_be_retrieved_realistically())

    def run_retrieval(self) -> None:
        logger.info("Retrieving files started")
        inp_file: str
        self.hsm_proxy.update_from_proxy()
        files_to_be_retrieved: set = (
            self.files_retrieval_requested
            - self.files_retrieval_succeeded
            - {f for f in self.files_retrieval_failed.keys()}
        )
        for inp_file in files_to_be_retrieved:
            if inp_file in self.hsm_proxy.files_recall_done:
                # skip files which do not need to be retrieved anymore
                out_dir: str = self.files_retrieval_destination[inp_file]
                Path(out_dir).mkdir(
                    parents=True, exist_ok=True, mode=self.file_permissions
                )
                # check if file should be retrieved or not
                output_dry_retrieve = pyslk.retrieve_improved(
                    inp_file, out_dir, dry_run=True, preserve_path=False
                )
                self._eval_output_dry_retrieval(
                    output_dry_retrieve,
                    inp_file,
                    out_dir,
                )
        logger.info(
            f"{len(self.files_retrieval_succeeded)}/"
            + f"{len(self.files_retrieval_requested)} files retrieved"
        )

    def _eval_output_dry_retrieval(
        self,
        output_retrieve: dict,
        inp_file: str,
        out_dir: str,
    ) -> None:
        # example output of pyslk.retrieve_improved:
        """
        {
            'SKIPPED': {
                'SKIPPED_TARGET_EXISTS': ['/arch/bm0146/k204221/iow/INDEX.txt']
            },
            'FILES': {
                '/arch/bm0146/k204221/iow/INDEX.txt': '/home/k204221/tmp/INDEX.txt'}
            }

        # dry run
        {
            'ENVISAGED': {'ENVISAGED': ['/arch/bm0146/k204221/iow/INDEX.txt']},
            'FILES': {
                '/arch/bm0146/k204221/iow/INDEX.txt':
                    '/home/k204221/tmp/abcdef2/INDEX.txt'}
            }

        # after successful retrieval
        {
            'ENVISAGED': {
                'ENVISAGED': []
            },
            'FILES': {
                '/arch/bm0146/k204221/iow/INDEX.txt':
                    '/home/k204221/tmp/INDEX.txt'
            },
            'SUCCESS': {
                'SUCCESS':
                    ['/arch/bm0146/k204221/iow/INDEX.txt']
            }
        }

        {
            'FAILED': {
                'FAILED_NOT_CACHED': ['/arch/bm0146/k204221/iow/iow_data5_001.tar']
            },
            'FILES': {
                '/arch/bm0146/k204221/iow/iow_data5_001.tar':
                    '/home/k204221/tmp/iow_data5_001.tar'
            }
        }
        """
        if "ENVISAGED" in output_retrieve:
            # we can try to retrieve the file
            # message on which file is retrieved to where
            logger.debug(f"Retrieving file {inp_file} to {out_dir}")
            # new retrieve command
            output_retrieve = pyslk.retrieve_improved(
                inp_file, out_dir, dry_run=False, preserve_path=False
            )
            if "SUCCESS" in output_retrieve:
                # check if file was successfully retrieved
                logger.debug(
                    f"File {inp_file} was successfully retrieved to {out_dir}."
                )
                logger.debug("Adjusting file permissions")
                Path(
                    os.path.join(os.path.expanduser(out_dir), Path(inp_file).name)
                ).chmod(self.file_permissions)
                self.files_retrieval_succeeded.add(inp_file)
                # we only return in the case of a 'success'; else we proceed
                return None

        # NOTE: here is no 'elif' but 'if' because we capture this case below:
        #           "dry run" -> 'ENVISAGED' -> "retrieve" -> 'FAILED'
        # check if file should be skipped
        if "SKIPPED" in output_retrieve:
            logger.debug(f"File {inp_file} does already exist in {out_dir}. Skip.")
            self.files_retrieval_succeeded.add(inp_file)
        elif "FAILED" in output_retrieve:
            # check if file somehow cannot be retrieved
            if "FAILED_NOT_CACHED" in output_retrieve["FAILED"]:
                logger.debug(f"File {inp_file} is not cached although it should be.")
                self.files_unexpectedly_not_cached.add(inp_file)
            else:
                logger.error(
                    f"File {inp_file} cannot be retrieved for unknown reasons. "
                    + "Ignore."
                )
                self.files_retrieval_failed[inp_file] = next(
                    iter(output_retrieve["FAILED"])
                )
        else:
            logger.error(
                f"Retrieval request for file {inp_file} yielded unexpected output. "
                + f"Ignore. Output: {output_retrieve}"
            )
            self.files_retrieval_failed[inp_file] = (
                "unexpected JSON output of pyslk.retrieve_improved: "
                + f"{json.dumps(output_retrieve)}"
            )


def _write_file_lists(
    hsm_proxy: HSMProxy, slk_retrieval: SLKRetrieval, slk_cache: Path
) -> None:
    # use only files which were actually requested
    # (the recall job might target more files!)
    files_recall_failed: set[str] = hsm_proxy.files_broken.intersection(
        slk_retrieval.files_retrieval_requested
    )
    files_not_retrieved_yet: set[str] = (
        slk_retrieval.get_files_still_to_be_retrieved_realistically()
    )
    tmp_str: str
    logger.debug(
        "Start writing file lists if one of these values is larger than 0: #'files "
        + f"retrieval reasonable' is {len(files_not_retrieved_yet)}; #'files recall "
        + f"failed' is {len(files_recall_failed)}; #'files retrieval failed' is "
        + f"{len(slk_retrieval.files_retrieval_failed.keys())}"
    )
    if (
        len(files_not_retrieved_yet) > 0
        or len(files_recall_failed) > 0
        or len(slk_retrieval.files_retrieval_failed.keys()) > 0
    ):
        logger.debug(
            f"#files_not_retrieved: {len(files_not_retrieved_yet)}"
            + f"#files_failed_recall: {len(files_recall_failed)}"
            + "#files_failed_retrieve: "
            + f"{len(slk_retrieval.files_retrieval_failed.keys())}"
        )
        timestamp: str = datetime.now().strftime("%Y%m%dT%H%M%S")
        output_name_suffix: str = f"_{timestamp}.txt"
        output_name_failed_recall: str = f"files_failed_recall{output_name_suffix}"
        output_name_failed_retrieve: str = f"files_failed_retrieve{output_name_suffix}"
        output_name_not_retrieved_yet: str = (
            f"files_failed_retrieve{output_name_suffix}"
        )
        logger.error(
            "One or more files could not be retrieved from the tape archive. They "
            + f"are printed below and written into files '*{output_name_suffix}'"
            + f"in directory '{str(slk_cache)}'."
        )
        if len(files_recall_failed) > 0:
            logger.debug(
                "Write list of files which recall failed into "
                + f"{os.path.join(slk_cache, output_name_failed_recall)}"
            )
            tmp_str = "\n  ".join(files_recall_failed)
            logger.error(f"files, recall failed:\n  {tmp_str}")
            with open(os.path.join(slk_cache, output_name_failed_recall), "w") as f:
                for file_path in files_recall_failed:
                    f.write(f"{file_path}: recall failed\n")
        if len(slk_retrieval.files_retrieval_failed) > 0:
            logger.debug(
                "Write list of files which retrieval failed into "
                + f"{os.path.join(slk_cache, output_name_failed_retrieve)}"
            )
            # TODO: does this work with dict?
            tmp_str = "\n  ".join(slk_retrieval.files_retrieval_failed)
            logger.error(f"files, retrieval failed (recall successful):\n  {tmp_str}")
            with open(os.path.join(slk_cache, output_name_failed_retrieve), "w") as f:
                for (
                    file_path,
                    reason,
                ) in slk_retrieval.files_retrieval_failed.items():
                    f.write(f"{file_path}: {reason}\n")
        if len(files_not_retrieved_yet) > 0:
            logger.debug(
                "Write list of files which were not retrieved for other "
                + f"reasons to {os.path.join(slk_cache, output_name_not_retrieved_yet)}"
            )
            tmp_str = "\n  ".join(files_not_retrieved_yet)
            logger.error(f"files, missing for other reasons:\n  {tmp_str}")
            with open(os.path.join(slk_cache, output_name_not_retrieved_yet), "w") as f:
                for file_path in files_not_retrieved_yet:
                    f.write(f"{file_path}: failed for unknown reasons\n")


def _mkdirs(path: Union[str, Path], dir_permissions: int) -> None:
    rp = os.path.realpath(path)
    if os.access(rp, os.F_OK):
        if not os.access(rp, os.W_OK):
            raise PermissionError(
                f"Cannot write to directory, {rp}, needed for downloading data. "
                + "Probably, you lack access privileges."
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
                    f"Cannot create or access directory, {e.filename}, needed for "
                    + "downloading data."
                )
            os.chmod(subpath, dir_permissions)


def _reformat_retrieve_files_list(
    retrieve_files: set[tuple[str, str]], dir_permissions: int
) -> set[tuple[str, str]]:
    retrieve_files_corrected: set[tuple[str, str]] = set()
    for inp_file, out_dir in retrieve_files:
        _mkdirs(out_dir, dir_permissions)
        # this `mkdir` indirectly sets proper access permissions for this folder
        out_file: str = os.path.join(os.path.expanduser(out_dir), Path(inp_file).name)
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
        retrieve_files_corrected.add((str(inp_file), str(out_dir)))

    return retrieve_files_corrected
