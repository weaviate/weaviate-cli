#!/bin/bash

set -e

echo "$HOME/.config/semi_technologies/"
mkdir -p "$HOME/.config/semi_technologies/"
echo '{"url": "http://localhost:8080", "auth": null}' > "$HOME.config/semi_technologies/configs.json"

