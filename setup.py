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
import subprocess
import setuptools
from setuptools.command.install import install
from setuptools import Distribution

with open("README.md", "r") as fh:
    longDescription = fh.read()

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        subprocess.call("echo " + getSetuptoolsScriptDir() + "/weaviate-cli.py > /usr/local/bin/weaviate-cli && chmod +x /usr/local/bin/weaviate-cli", shell=True)
        install.run(self)

class OnlyGetScriptPath(install):
    """Get the script path."""
    def run(self):
        self.distribution.install_scripts = self.install_scripts

def getSetuptoolsScriptDir():
    " Get the directory setuptools installs scripts to for current python "
    dist = Distribution({'cmdclass': {'install': OnlyGetScriptPath}})
    dist.dry_run = True  # not sure if necessary
    dist.parse_config_files()
    command = dist.get_command_obj('install')
    command.ensure_finalized()
    command.run()
    return dist.install_scripts

setuptools.setup(
    name='weaviate-cli',
    version='0.0.3',
    scripts=['weaviate-cli.py'],
    author="SeMI Technologies",
    author_email="hello@semi.technology",
    description="CLI tool for Weaviate",
    long_description=longDescription, long_description_content_type="text/markdown",
    url="https://github.com/semi-technologies/weaviate-cli",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    cmdclass={
        'install': PostInstallCommand,
    }
)
