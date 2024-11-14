# Weaviate CLI

<img src="https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png" width="180" align="right" alt="Weaviate logo">

[![Build Status](https://github.com/weaviate/weaviate-cli/actions/workflows/main.yaml/badge.svg)](https://github.com/weaviate/weaviate-cli/actions/workflows/main.yaml)
[![PyPI version](https://badge.fury.io/py/weaviate-cli.svg)](https://badge.fury.io/py/weaviate-cli)

A powerful command-line interface for managing and interacting with Weaviate vector databases directly from your terminal.

## Key Features
- **Collections**: Create, update, delete and get collection configurations
- **Data Management**: Import, query, update and delete data with various search types (vector, keyword, hybrid)
- **Multi-tenancy**: Manage tenants and their states across collections
- **Backup & Restore**: Create and restore backups with support for S3, GCS and filesystem
- **Sharding**: Monitor and manage collection shards
- **Flexible Configuration**: Configure vector indexes, replication, consistency levels and more

## Quick Start
Install using pip:

```bash
pip install weaviate-cli
```

## Basic Usage

```bash
# Show available commands
weaviate-cli --help

# Create a collection
weaviate-cli create collection --collection movies --vectorizer transformers

# Import test data
weaviate-cli create data --collection movies --limit 1000

# Query data
weaviate-cli query data --collection movies --search-type hybrid --query "action movies"
```

## Core Commands

- **create**: Create collections, tenants, backups or import data
- **delete**: Remove collections, tenants or data
- **update**: Modify collection settings, tenant states or data
- **get**: Retrieve collection info, tenant details or shard status
- **query**: Search data using various methods
- **restore**: Restore backups from supported backends

## Configuration

Weaviate CLI allows you to configure your cluster endpoints and parameters through a configuration file. By default, the CLI looks for a
configuration file at `~/.config/weaviate/config.json`. If this file does not exist, it will be created with the following default values:

```json
{
    "host": "localhost",
    "http_port": "8080",
    "grpc_port": "50051"
}
```

You can also specify your own configuration file using the `--config-file` option:

```bash
weaviate-cli --config-file /path/to/your/config.json
```

The configuration file should be a JSON file with the following structure:

```json
{
    "host": "your-weaviate-host",
    "http_port": "your-http-port",
    "grpc_port": "your-grpc-port",
    "auth": {
        "type": "api_key",
        "api_key": "your-api-key"
    }
}
```

If you are using a remote Weaviate instance, you can use the `weaviate-cli` command to authenticate with your Weaviate instance.
Here you can see an example on how the configuration file should look like if you are connecting to a WCD cluster:

```json
 {
     "host": "thisisaninventedcluster.url.s3.us-west3.prov.weaviate.cloud",
     "auth": {
         "type": "api_key",
         "api_key": "jfeRFsdfRfSasgsDoNOtTrYToUsErRQwqqdZfghasd"
     },
    "headers":{
        "X-OpenAI-Api-Key":"OPEN_AI_KEY",
        "X-Cohere-Api-Key":"Cohere_AI_KEY",
        "X-JinaAI-Api-Key":"JINA_AI_KEY"
        }
 }
```

## Requirements

- Python 3.9+
- Weaviate instance (local or remote)

## Documentation

Detailed documentation will be added soon.

## Supported Model Provider

- Contextionary
- Transformers
- OpenAI
- Ollama
- Cohere
- JinaAI

## Community & Support

- [Slack Community](https://weaviate.io/slack) - Join our active community
- [Stack Overflow](https://stackoverflow.com/questions/tagged/weaviate) - Search using the `weaviate` tag
- [GitHub Issues](https://github.com/weaviate/weaviate-cli/issues) - Report bugs or request features

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](https://github.com/weaviate/weaviate-cli/blob/main/CONTRIBUTING.md) for
details.

## License

BSD-3-Clause License
