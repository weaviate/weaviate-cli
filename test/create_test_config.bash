#!/bin/bash

set -e

mkdir -p $HOME.config/semi_technologies/
echo '{"url": "http://localhost:8080", "auth": null}' > "$HOME.config/semi_technologies/configs.json"

