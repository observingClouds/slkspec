from fsspec import register_implementation

from . import _version
from .core import SLKFileSystem

__version__ = _version.get_versions()["version"]

register_implementation(SLKFileSystem.protocol, SLKFileSystem)

__all__ = ["__version__", "SLKFileSystem"]
