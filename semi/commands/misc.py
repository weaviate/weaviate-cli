from semi.config.configuration import Configuration
from semi.version import __version__


def ping(cfg: Configuration) -> None:
    """
    Get weaviate ping status.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration. 
    """

    if (cfg.client.is_ready()):
        print("Weaviate is reachable!")
    else:
        print("Weaviate not reachable!")


def version() -> None:
    """
    Print weaviate CLI version.
    """

    print(__version__)
