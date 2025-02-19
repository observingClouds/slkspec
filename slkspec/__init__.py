from fsspec import register_implementation

from importlib.metadata import version
from .core import SLKFileSystem, logger

__version__ = version("slkspec")

register_implementation(SLKFileSystem.protocol, SLKFileSystem)

__all__ = ["__version__", "SLKFileSystem", "logger"]
