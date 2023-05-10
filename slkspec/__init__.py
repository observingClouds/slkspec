from fsspec import register_implementation

from . import _version
from .core import SLKFileSystem, logger

__version__ = _version.get_versions()["version"]

register_implementation(SLKFileSystem.protocol, SLKFileSystem)

__all__ = ["__version__", "SLKFileSystem", "logger"]

from . import _version
__version__ = _version.get_versions()['version']
