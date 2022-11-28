from __future__ import annotations
import logging
import io
import os
from pathlib import Path
from queue import Queue
import threading
from typing import AnyStr, Any, BinaryIO, IO, Literal, Optional, TextIO, Union

from fsspec.spec import AbstractFileSystem

# from pyslk import pyslk as pslk

logger = logging.getLogger("slkspec")

MAX_RETRIES = 2
FileQueue: Queue[str] = Queue(maxsize=-1)
_retrieval_lock = threading.Lock()


class pyslk:
    @staticmethod
    def slk_list(inp: str | Path) -> str:
        from subprocess import run, CalledProcessError, PIPE

        return run(
            ["ls", "-l", f"{Path(inp).expanduser().absolute()}"],
            stdout=PIPE,
            stderr=PIPE,
        ).stdout.decode()

    @staticmethod
    def slk_retrieve(items: list[str]) -> None:
        import shutil
        import time

        print(f"Retrieving {len(items)} items from tape")
        time.sleep(len(items) / 2)
        # Path(out).parent.mkdir(exist_ok=True, parents=True)
        # shutil.copy(inp, out)


class SLKFile(io.IOBase):
    """File handle for files on the hsm archive.

    Parameters
    ----------
    file: str | pathlib.Path
        path to the files.
    mode: str, default: rb
        specify the mode in which the files are opened

        'r'       open for reading (default)
        'b'       binary mode
        't'       text mode (default)

    **kwargs
        Additional keyword arguments passed to the open file descriptor method.
    """

    write_msg: str = "Write mode is not suppored"
    """Error message that is thrown if the files are attempted to be opened
    in any kind of write mode."""

    def __init__(
        self,
        url: str | Path,
        local_file: str | Path,
        *,
        override: bool = True,
        mode: str = "rb",
        touch: bool = True,
        file_mod: int = 0o3777,
        _lock: threading.Lock = _retrieval_lock,
        _file_queue: Queue[tuple[str, str]] = FileQueue,
        **kwargs,
    ):
        if not set(mode) & set("r"):  # The mode must have a r
            raise NotImplementedError(self.write_msg)
        if "b" not in mode:
            kwargs.setdefault("encoding", "utf-8")
        self._file = str(Path(local_file).expanduser().absolute())
        self._url = str(url)
        self.touch = touch
        self.file_mod = file_mod
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
        with _lock:
            if not Path(self._file).exists() or override:
                self._file_queue.put((self._url, str(Path(self._file).parent)))
            elif Path(self._file).exists():
                self._file_obj = open(self._file, mode, **kwargs)

    @property
    def name(self) -> str:
        """Get the file for the SLKFile object."""
        if self._file_obj is not None:
            return self._file
        return self._url

    def _retrieve_items(self, retrieve_files: list[tuple[str, str]]) -> None:
        """Get items from the tape archive."""
        import shutil

        print(f"Retrieving {len(retrieve_files)} items from tape")
        for inp_file, out_dir in retrieve_files:
            Path(out_dir).mkdir(
                exist_ok=True, parents=True, mode=self.file_mod
            )
            target = Path(out_dir) / Path(inp_file).name
            shutil.copy(inp_file, target)
            target.chmod(self.file_mod)

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
        return self._file_obj.tell()

    def seek(self, target: int) -> int:
        if self._file_obj is None:
            return self._file_obj.seek(target)
        self._cache_files()
        return self._file_obj.seek(target)

    @staticmethod
    def readable() -> Literal[True]:
        """Comptibility method."""
        return True

    @staticmethod
    def writeable() -> Literal[False]:
        """Comptibility method."""
        return False

    @staticmethod
    def seekable() -> Literal[True]:
        """Comptibility method."""
        return True

    def read(self, size: int = -1) -> AnyStr:
        """The the content of a file-stream.

        size: int, default: -1
            read at most size characters from the stream, -1 means everything
            is read.
        """
        if self._file_obj is None:
            self._cache_files()
        return self._file_obj.read(size)

    @staticmethod
    def flush() -> None:
        """Flushing file systems shouldn't work for ro modes."""
        return None

    def writelines(self, *arg) -> None:
        """Comptibility method."""
        raise NotImplementedError(self.write_msg)

    def write(self, *arg) -> None:
        """Writing to tape is not supported."""
        raise NotImplementedError(self.write_msg)

    def close(self) -> None:
        if self._file_obj is not None:
            self._file_obj.close()


class SLKFileSystem(AbstractFileSystem):
    protocol = "slk"
    sep = "/"

    def __init__(
        self,
        auth=None,
        block_size=None,
        local_cache=None,
        touch: bool = True,
        override: bool = False,
        client_kwargs=None,
        **storage_options,
    ):
        super().__init__(
            block_size=block_size,
            asynchronous=False,
            loop=None,
            **storage_options,
        )
        local_cache = local_cache or os.environ.get("SLK_CACHE")
        if not local_cache:
            raise KeyError(
                "Local cache should be given. As an alternative set environment variable SLK_CACHE."
            )
        self.touch = touch
        self.local_cache = Path(local_cache)
        self.client_kwargs = client_kwargs or {}
        self.override = override

    def ls(self, path, detail=True, **kwargs):
        filelist = pyslk.slk_list(path).split("\n")
        detail_list = []
        types = {"d": "directory", "-": "file"}
        for file_entry in filelist[:-2]:
            entry = {
                "name": path
                + "/"
                + file_entry.split(" ")[
                    -1
                ],  # this will fail if filenames contains whitespaces
                "size": None,  # sizes are human readable not in bytes
                "type": types[file_entry[0]],
            }
            detail_list.append(entry)
        if detail:
            return detail_list
        else:
            return [d["name"] for d in detail_list]

    def cat_file(self, path, start=None, end=None, touch=False, mod="3o777"):
        """Get file content

        Inputs
        ------
        path : str
          file to open
        start : int
          seek position within file, optional
        end : int
          end position of file read, optional
        touch : bool
          switch to update file modification and access times of local
          files at every request. This option might be helpful to
          keep retrievals from tape to a minimum and prevent often
          used files from being garbage collected.

        Returns
        -------
        - bytes if start and end are given
        - file object otherwise
        """
        slk_file = self._open(path, mode="rb")
        if isinstance(start, int) and isinstance(end, int):
            return slk_file.seed(end - start)
        return slk_file

    def _open(
        self,
        path: str | Path,
        mode: str = "rb",
        encoding: Optional[str] = None,
        **kwargs,
    ):
        path = Path(self._strip_protocol(path))
        local_path = self.local_cache.joinpath(*path.parts[1:])
        return SLKFile(
            path,
            local_path,
            mode=mode,
            override=self.override,
            touch=self.touch,
            encoding=encoding,
        )
