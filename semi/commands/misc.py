from semi.config.configuration import Configuration
import subprocess

_version_failed_answer = "The installed cli version can not be assessed! Run `pip show weaviate-cli` to view the version manually"


def ping(cfg:Configuration):
    if (cfg.client.is_ready()):
        print("Weaviate is reachable!")
    else:
        print("Weaviate not reachable!")


def version():
    out = _get_pip_output()
    print(_parse_version_from_output(out))


def _parse_version_from_output(output:str):
    pre_processed = output.replace('\n', ' ').replace('\r', '').lower()
    version_start = pre_processed.find("version: ")
    if version_start == -1:
        return _version_failed_answer

    version = pre_processed[version_start+9:]
    version_end = version.find(" ")
    if version_end == -1:
        return _version_failed_answer
    return version[:version_end]


def _get_pip_output():
    result = subprocess.run(['pip', 'show', 'weaviate-cli'], stdout=subprocess.PIPE)
    return result.stdout.decode("utf-8")