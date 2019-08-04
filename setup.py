##                          _       _
##__      _____  __ ___   ___  __ _| |_ ___
##\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
## \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
##  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
##
## Copyright © 2016 - 2018 Weaviate. All rights reserved.
## LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
## AUTHOR: Bob van Luijt (bob@kub.design)
## See www.creativesoftwarefdn.org for details
## Contact: @CreativeSofwFdn / bob@kub.design
##
"""This is the setup module for the Weaviate-cli tool."""
"""Based on: https://dzone.com/articles/executable-package-pip-install"""
import setuptools
from setuptools.command.install import install
from setuptools import Distribution

with open("README.md", "r") as fh:
    longDescription = fh.read()

with open("version", "r") as fh:
    version = fh.read()

setuptools.setup(name='weaviate-cli',
    version=version,
    description="CLI tool for Weaviate",
    long_description=longDescription, long_description_content_type="text/markdown",
    url="https://semi.technology/",
    author="SeMI Technologies",
    author_email="hello@semi.technology",
    license='GPLv3',
    packages=setuptools.find_packages(),
    # entry_points = {'console_scripts': ['weaviate-cli = weaviate-cli:main']},
    entry_points={
        "console_scripts": [
            "weaviate-cli=__main__:main",
        ]
    },
    keywords = ['Weaviate'],
    zip_safe=False)