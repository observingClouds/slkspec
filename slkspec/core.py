import logging
import os
from pathlib import Path

import pyslk
from fsspec.spec import AbstractFileSystem
from pyslk import pyslk as pslk

logger = logging.getLogger("slkspec")

MAX_RETRIES = 2


class SLKFileSystem(AbstractFileSystem):
    protocol = "slk"
    sep = "/"

    def __init__(
        self,
        auth=None,
        block_size=None,
        local_cache=None,
        asynchronous=False,
        loop=None,
        client_kwargs=None,
        verify_uploads=True,
        **storage_options,
    ):
        super().__init__(
            block_size=block_size,
            asynchronous=asynchronous,
            loop=loop,
            **storage_options,
        )
        if local_cache is None and os.environ.get("SLK_CACHE", None) is None:
            raise KeyError(
                "Local cache should be given. As an alternative set environment variable SLK_CACHE."
            )
        elif local_cache is not None:
            self.local_cache = local_cache
        elif os.environ.get("SLK_CACHE", None) is not None:
            self.local_cache = os.environ["SLK_CACHE"]
        self.client_kwargs = client_kwargs or {}

    def ls(self, path, detail=True, **kwargs):
        filelist = pslk.slk_list(path).split("\n")
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

    def cat_file(self, path, start=None, end=None, touch=False):
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
        path = path.replace("slk://", "")
        try:
            f = open(self.local_cache + path, "rb")
            print(f"{path} found locally.")
            if touch:
                Path(path).touch()
            if start is not None and end is not None:
                f.seek(start)
                bytes = f.read(end - start)
                return bytes
            else:
                return f
        except FileNotFoundError:
            print(f"{path} needs to be retrieved from tape.")
            self._get_file(self.local_cache + os.path.dirname(path), path)
            f = open(self.local_cache + path, "rb")
            if start is not None and end is not None:
                f.seek(start)
                bytes = f.read(end - start)
                return bytes
            else:
                return f

    def _get_file(self, lpath, rpath, **kwargs):
        pyslk.retrieve(rpath, lpath, preserve_path=False, skip_exists=True)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        local_path = os.environ["SLK_CACHE"]
        path = path.replace("slk://", "")
        try:
            print("local path")
            return open(local_path + path, "rb")
        except FileNotFoundError:
            print("retrieval")
            self._get_file(local_path + os.path.dirname(path), path)
            return open(local_path + path, "rb")
