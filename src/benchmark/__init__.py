from importlib.metadata import metadata
from .benchmark import run_benchmark

meta = metadata("dggs-benchmarking")
__version__ = meta["Version"]
__author__ = meta["Author-email"]
__license__ = meta["License"]
__email__ = meta["Author-email"]
__program_name__ = meta["Name"]

__all__ = ["run_benchmark"]
