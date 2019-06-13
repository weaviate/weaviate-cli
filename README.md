# Weaviate-CLI <img alt='Weaviate logo' src='https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='180' align='right' />

_CLI tool for for Weaviate_

## Installation

For installation, choose a director where you want to install the CLI tool. And follow;

```sh
$ git clone
$ export PATH=$PATH:$(pwd)/weaviate-cli
$ echo 'export PATH=$PATH:'$(pwd)'/weaviate-cli' >> ~/.bashrc
```

After re-opening your CLI. You can use the cli-tool globally by running:

```sh
$ weaviate-cli --version
```

Note:<br>
requirements.txt is created with `pipreqs ./`

## Documentation

[Documentation can be found in the Weaviate repo.](https://github.com/semi-technologies/weaviate/blob/master/docs/en/use/weaviate-cli-tool.md)

## Build Status

| Badge   | Status        |
| -------- |:-------------:|
| Travis   | [![Build Status](https://api.travis-ci.org/semi-technologies/weaviate-cli.svg?branch=master)](https://travis-ci.org/creativesoftwarefdn/weaviate-cli/branches)
| PyPi     | [![Build Status](https://img.shields.io/pypi/v/weaviate-cli.svg)](https://pypi.org/project/weaviate-cli/)