from sys import version_info, exit
#check python version
if version_info.major < 3:
    exit("Python 3.x is required to run this program")

from os import path
from builtins import open
from setuptools import setup
from lib.version import __version__
# read the contents of your README file

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="weaviate-cli",
    version=__version__,
    description="Comand line interface to interact with weaviate",
    long_description=long_description,
    long_description_content_type='text/x-rst',
    author="SeMI Technologies",
    author_email="hello@semi.technology",
    packages=["lib", "lib.commands",  "lib.managers"],
    py_modules=['cli'],
    python_requires='>=3.8',
    install_requires=[
        "weaviate-client>=4.9.0",
        "click==8.1.7",
        "semver==3.0.2",],
    entry_points='''
    [console_scripts]
    weaviate=cli:main
    '''
)
