# Weaviate-CLI <img alt='Weaviate logo' src='https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='180' align='right' />

_CLI tool for for Weaviate_

## Installation

For installation, choose a director where you want to install the CLI tool. And follow;

```sh
$ git clone https://github.com/semi-technologies/weaviate-cli
$ pip install -r requirements.txt
$ ln -s $(pwd)/weaviate-cli/weaviate-cli.py /usr/local/bin/weaviate-cli
```

After re-opening your CLI. You can use the cli-tool globally by running:

```sh
$ weaviate-cli version
```

Note:<br>
- runs with python3 and pip3
- requirements.txt is created with `pipreqs ./`

## Build Status

| Badge   | Status        |
| -------- |:-------------:|
| Travis   | [![Build Status](https://api.travis-ci.org/semi-technologies/weaviate-cli.svg?branch=master)](https://travis-ci.org/creativesoftwarefdn/weaviate-cli/branches)