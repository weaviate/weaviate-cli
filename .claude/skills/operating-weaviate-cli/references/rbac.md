# RBAC (Roles, Users, Permissions) Reference

Manage role-based access control in Weaviate.

## Complete RBAC Setup
```bash
# 1. Create a role with permissions
weaviate-cli create role --role_name MoviesAdmin \
  -p crud_collections:Movies \
  -p crud_data:Movies \
  -p crud_tenants:Movies \
  --json

# 2. Create a user (returns API key)
weaviate-cli create user --user_name test-user --json

# 3. Assign role to user (supports multiple --role_name)
weaviate-cli assign role --role_name MoviesAdmin --user_name test-user --json
weaviate-cli assign role --role_name MoviesAdmin --role_name TenantAdmin --user_name test-user --json

# 4. Verify
weaviate-cli get role --role_name MoviesAdmin --json
weaviate-cli get user --user_name test-user --json
```

## Inspect Roles
```bash
weaviate-cli get role --all --json                              # All roles
weaviate-cli get role --role_name MoviesAdmin --json            # Specific role
weaviate-cli get role --user_name test-user --json              # Roles of a user
weaviate-cli get role --user_name oidc-user --user_type oidc --json  # OIDC user's roles
```

## Inspect Users
```bash
weaviate-cli get user --json                                    # Current user
weaviate-cli get user --all --json                              # All users
weaviate-cli get user --user_name test-user --json              # Specific user
weaviate-cli get user --role_name MoviesAdmin --json            # Users with role
```

## Modify Permissions
```bash
weaviate-cli assign permission -p read_data:Movies --role_name MoviesAdmin --json
weaviate-cli revoke permission -p read_data:Movies --role_name MoviesAdmin --json
```

## Manage User Lifecycle
```bash
weaviate-cli create user --user_name test-user --store --json   # Create and store API key in config
weaviate-cli update user --user_name test-user --rotate_api_key --json
weaviate-cli update user --user_name test-user --rotate_api_key --store --json  # Rotate and store
weaviate-cli update user --user_name test-user --deactivate --json
weaviate-cli update user --user_name test-user --activate --json
weaviate-cli delete user --user_name test-user --json
```

## Cleanup
```bash
weaviate-cli revoke role --role_name MoviesAdmin --user_name test-user --json
weaviate-cli revoke role --role_name MoviesAdmin --user_name oidc-user --user_type oidc --json  # OIDC user
weaviate-cli delete role --role_name MoviesAdmin --json
weaviate-cli delete user --user_name test-user --json
```

## Permission Format

Permissions use the format `action:target`. Multiple permissions can be specified with repeated `-p` flags.

**Available actions:**

| Category | Actions |
|----------|---------|
| Collections | `create_collections`, `read_collections`, `update_collections`, `delete_collections` |
| Data | `create_data`, `read_data`, `update_data`, `delete_data` |
| Tenants | `create_tenants`, `read_tenants`, `update_tenants`, `delete_tenants` |
| Roles | `create_roles`, `read_roles`, `update_roles`, `delete_roles` |
| Users | `assign_and_revoke_users`, `read_users` |
| Aliases | `create_aliases`, `read_aliases`, `update_aliases`, `delete_aliases` |
| Cluster | `read_cluster` |
| Nodes | `read_nodes` |
| Backups | `manage_backups` |

**CRUD shorthands:** `crud_collections`, `crud_data`, `crud_tenants`, `crud_roles`, `crud_users`, `crud_aliases`, `cud_data`, `rd_data`, `cud_tenants`, `rd_tenants`, `rd_collections`, `cud_aliases`, `rd_aliases`

Any combination of `c`, `r`, `u`, `d` prefix letters works (e.g., `cr_collections` for create+read).

**Examples:**
```
-p crud_collections:Movies
-p cud_tenants:Person_*
-p crud_data:Movies,Books
-p crud_tenants:Movies,Books:MyTenant*,YourTenant*
-p read_nodes:verbose:Movies
-p read_nodes:minimal
-p manage_backups:Movies
-p read_cluster
```

## Prerequisites

1. Cluster must have RBAC enabled
2. Connecting user must have sufficient permissions to manage roles/users
3. For `--user` global option: config must have auth type `user`

## Notes

- `create user` returns an API key -- save it, as it cannot be retrieved later
- `--store` saves the API key in the config file (requires auth type `user`)
- `--user_type` defaults to `db`; use `oidc` for OIDC-authenticated users (applies to `get role`, `assign role`, `revoke role`)
- `--role_name` supports `multiple=True` for `assign role` and `revoke role` (assign/revoke multiple roles at once)
- Role names and user names are case-sensitive
