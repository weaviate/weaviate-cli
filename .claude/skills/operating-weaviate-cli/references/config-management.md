# Config Management Reference

Patterns for managing weaviate-cli configuration files.

## Config File Format

```json
{
    "host": "hostname-or-ip",
    "http_port": "8080",
    "grpc_port": "50051",
    "grpc_host": "optional-separate-grpc-host",
    "auth": {
        "type": "api_key|user",
        "api_key": "key-for-api_key-type",
        "user-name": "key-for-user-type"
    }
}
```

## Decision Tree: When to Create a Config

1. **Local cluster, no auth** -- No config needed. The CLI defaults to `localhost:8080`/`50051`.
2. **Local cluster with RBAC** -- Create a config with auth type `api_key` or `user`.
3. **Remote cluster (Weaviate Cloud)** -- Create a config with the cluster URL and API key.
4. **Custom host/ports** -- Create a config specifying host, ports, and optional auth.

## Config File Naming Convention

Store configs under `~/.config/weaviate/` with descriptive filenames:

| Scenario | Filename |
|----------|----------|
| Default local | (none needed) |
| Local with RBAC | `local-rbac.json` |
| Staging cloud cluster | `cloud-staging.json` |
| Production cloud cluster | `cloud-prod.json` |
| Custom environment | `custom-<env>.json` |

## Common Patterns

### Local (no config)
```bash
weaviate-cli get nodes --minimal --json
```

### Local with API key auth
```bash
cat > ~/.config/weaviate/local-auth.json << 'EOF'
{
    "host": "localhost",
    "http_port": "8080",
    "grpc_port": "50051",
    "auth": {
        "type": "api_key",
        "api_key": "admin-key"
    }
}
EOF
weaviate-cli --config-file ~/.config/weaviate/local-auth.json get nodes --minimal --json
```

### Local with RBAC (per-user auth)
```bash
cat > ~/.config/weaviate/local-rbac.json << 'EOF'
{
    "host": "localhost",
    "http_port": "8080",
    "grpc_port": "50051",
    "auth": {
        "type": "user",
        "admin-user": "admin-key"
    }
}
EOF
weaviate-cli --config-file ~/.config/weaviate/local-rbac.json --user admin-user get nodes --minimal --json
```

### Weaviate Cloud
```bash
cat > ~/.config/weaviate/cloud-staging.json << 'EOF'
{
    "host": "https://your-cluster.weaviate.cloud",
    "auth": {
        "type": "api_key",
        "api_key": "your-api-key"
    }
}
EOF
weaviate-cli --config-file ~/.config/weaviate/cloud-staging.json get nodes --minimal --json
```

## Using the In-Development CLI Version

When testing code changes or validating in-dev features, activate the development venv first before running `weaviate-cli` commands:

```bash
source <weaviate-cli-repo>/.venv/bin/activate
weaviate-cli get nodes --minimal --json
```

The user will tell you where the repo lives. Always use this venv when the task involves testing or verifying modifications to the weaviate-cli source code itself.

## Rules for Agents

- **Never overwrite** `~/.config/weaviate/config.json` without explicit user consent
- Use descriptive filenames -- not generic ones
- Always verify connectivity after creating a config
- If the user specifies a config file path, use it exactly
- If no config is specified, assume local defaults (no `--config-file` flag)
- When creating a user with `--store`, the API key is saved into the active config file
- When testing in-dev features, source the repo's `.venv/bin/activate` before running commands
