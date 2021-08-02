from os import path
from builtins import open
from setuptools import setup
from semi.version import __version__
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
    packages=["semi", "semi.config", "semi.commands"],
    py_modules=['cli'],
    python_requires='>=3.6',
    install_requires=[
        "weaviate-client>=2.1.0",
        "click==7.1.2"],
    entry_points='''
    [console_scripts]
    weaviate=cli:main
    '''
)
