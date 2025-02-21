import importlib.metadata

from fsspec import register_implementation

from .core import SLKFileSystem, logger

__version__ = importlib.metadata.version("slkspec")

register_implementation(SLKFileSystem.protocol, SLKFileSystem)

__all__ = ["__version__", "SLKFileSystem", "logger"]
