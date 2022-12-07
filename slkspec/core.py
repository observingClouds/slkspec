from __future__ import annotations
from collections import defaultdict
from getpass import getuser
import logging
import io
import os
from pathlib import Path
from queue import Queue
import re
import threading
from typing import (
    AnyStr,
    Any,
    Dict,
    IO,
    Literal,
    List,
    Optional,
    overload,
    Tuple,
    TypedDict,
    Union,
)
import warnings

from fsspec.spec import AbstractFileSystem
from pyslk import pyslk


logger = logging.getLogger("slkspec")
logger.setLevel(logging.INFO)


MAX_RETRIES = 2
FileQueue: Queue[Tuple[str, str]] = Queue(maxsize=-1)
FileInfo = TypedDict("FileInfo", {"name": str, "size": Literal[None], "type": str})
_retrieval_lock = threading.Lock()


class SLKFile(io.IOBase):
    """File handle for files on the hsm archive.

    Parameters
    ----------
    url: str
        Source path of the file that should be retrieved.
    local_file: str
        Destination path of the downloaded file.
    override: bool, default: False
        Override existing files
    touch: bool, default: True
        Update existing files on the temporary storage to prevent them
        from being deleted.
    mode: str, default: rb
        Specify the mode in which the files are opened

        'r'       open for reading (default)
        'b'       binary mode (default)
        't'       text mode
    file_permissions: int, default 0o3777
        Permission when creating directories and files.
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
        *,
        override: bool = True,
        mode: str = "rb",
        touch: bool = True,
        file_permissions: int = 0o3777,
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
        self.touch = touch
        self.file_permissions = file_permissions
        self._order_num = 0
        self._file_obj: Optional[IO[Any]] = None
        self._lock = _lock
        self.kwargs = kwargs
        self.mode = mode
        self.newlines = None
        self.error = "strict"
        self.encoding = kwargs.get("encoding")
        self.write_through = False
        self._file_queue = _file_queue
        print(self._file)
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

    def _retrieve_items(self, retrieve_files: list[tuple[str, str]]) -> None:
        """Get items from the tape archive."""

        retrieval_requests: Dict[Path, List[str]] = defaultdict(list)
        logger.debug("Retrieving %i items from tape", len(retrieve_files))
        for inp_file, out_dir in retrieve_files:
            retrieval_requests[Path(out_dir)].append(inp_file)
        for output_dir, inp_files in retrieval_requests.items():
            output_dir.mkdir(parents=True, exist_ok=True, mode=self.file_permissions)
            logger.debug("Creating slk query for %i files", len(inp_files))
            search_str = pyslk.slk_search(pyslk.slk_gen_file_query(inp_files))
            search_id_re = re.search("Search ID: [0-9]*", search_str)
            if search_id_re is None:
                raise ValueError("No files found in archive.")
            search_id = int(search_id_re.group(0)[11:])
            logger.debug("Retrieving files for search id: %i", search_id)
            pyslk.slk_retrieve(search_id, str(output_dir))
            logger.debug("Adjusting file permissions")
            for out_file in map(Path, inp_files):
                (output_dir / out_file.name).chmod(self.file_permissions)

    def _cache_files(self) -> None:
        with self._lock:
            items = []
            if self._file_queue.qsize() > 0:
                self._file_queue.put(("finish", "finish"))
                for _ in range(self._file_queue.qsize() - 1):
                    items.append(self._file_queue.get())
                    self._file_queue.task_done()
                self._retrieve_items(items)
                _ = self._file_queue.get()
                self._file_queue.task_done()
        self._file_queue.join()
        self._file_obj = open(self._file, self.mode, **self.kwargs)

    def __fspath__(self) -> str:
        if self._file_obj is None:
            self._cache_files()
        return self.name

    def tell(self) -> int:
        if self._file_obj is not None:
            return self._file_obj.tell()
        self._cache_files()
        return self._file_obj.tell()  # type: ignore

    def seek(self, target: int) -> int:  # type: ignore
        if self._file_obj is None:
             self._cache_files()
        return self._file_obj.seek(target)

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

    def read(self, size: int = -1) -> AnyStr:
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
    file_permissions: int, default: 0o3777
        Permission when creating directories and files.
    override: bool, default: False
        Override existing files
    touch: bool, default: True
        Update existing files on the temporary storage to prevent them
        from being deleted.
    **storage_options:
        Additional options passed to the AbstractFileSystem class.
    """

    protocol = "slk"
    sep = "/"

    def __init__(
        self,
        block_size: Optional[int] = None,
        slk_cache: Optional[Union[str, Path]] = None,
        file_permissions: int = 0o3777,
        touch: bool = True,
        override: bool = False,
        **storage_options: Any,
    ):
        super().__init__(
            block_size=block_size,
            asynchronous=False,
            loop=None,
            **storage_options,
        )
        slk_cache = slk_cache or os.environ.get("SLK_CACHE")
        if not slk_cache:
            slk_cache = f"/scratch/{getuser()[0]}/{getuser()}"
            warnings.warn(
                "The slk_cache variable nor the SLK_CACHE environment"
                "variable wasn't set. Falling back to default "
                f"{slk_cache}",
                UserWarning,
                stacklevel=2,
            )
        self.touch = touch
        self.slk_cache = Path(slk_cache)
        self.override = override
        self.file_permissions = file_permissions

    @overload
    def ls(
        self, path: Union[str, Path], detail: Literal[True], **kwargs: Any
    ) -> List[FileInfo]:
        ...

    @overload
    def ls(
        self, path: Union[str, Path], detail: Literal[False], **kwargs: Any
    ) -> List[str]:
        ...

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
        filelist = pyslk.slk_list(str(path)).split("\n")
        detail_list: List[FileInfo] = []
        types = {"d": "directory", "-": "file"}
        for file_entry in filelist[:-2]:
            entry: FileInfo = {
                "name": str(path / " ".join(file_entry.split()[8:])),
                "size": None,  # sizes are human readable not in bytes
                "type": types[file_entry[0]],
            }
            detail_list.append(entry)
        if detail:
            return detail_list
        return [d["name"] for d in detail_list]

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
            mode=mode,
            override=self.override,
            touch=self.touch,
            encoding=kwargs.get("encoding"),
            file_permissions=self.file_permissions,
        )
