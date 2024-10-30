from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("weaviate-cli")
except PackageNotFoundError:
    __version__ = "unknown version"
