import subprocess
from semi.config.configuration import Configuration


_version_failed_answer = "The installed cli version can not be assessed! Run `pip show weaviate-cli` to view the version manually"


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

    out = _get_pip_output()
    print(_parse_version_from_output(out))


def _parse_version_from_output(output: str) -> str:
    """
    Parse `pip show weaviate-cli` and get the version.

    Parameters
    ----------
    output : str
        The `pip show weaviate-cli` output.

    Returns
    -------
    str
        The version or the error message.
    """

    pre_processed = output.replace('\n', ' ').replace('\r', '').lower()
    version_start = pre_processed.find("version: ")
    if version_start == -1:
        return _version_failed_answer

    version = pre_processed[version_start+9:]
    version_end = version.find(" ")
    if version_end == -1:
        return _version_failed_answer
    return version[:version_end]


def _get_pip_output() -> str:
    """
    Get `pip show weaviate-cli` output.

    Returns
    -------
    str
        The output as str.
    """
    result = subprocess.run(['pip', 'show', 'weaviate-cli'], stdout=subprocess.PIPE)
    return result.stdout.decode("utf-8")