# Namespaces Reference

Manage Weaviate namespaces, namespace-scoped DB users, and `manage_namespaces`
permissions. **Requires Weaviate 1.38.0+.**

## CRUD

```bash
weaviate-cli create namespace --name tenantswest --json
weaviate-cli create namespace --name tenantswest --home_node node1 --json
weaviate-cli get namespace --name tenantswest --json
weaviate-cli get namespace --all --json
weaviate-cli update namespace --name tenantswest --home_node node2 --json
weaviate-cli delete namespace --name tenantswest --json
```

### Namespace name rules

A namespace name must match the server-side validation:

- 3 to 36 characters long
- only lowercase letters and digits (regexp: `[a-z][a-z0-9]*`)
- must start with a letter

The CLI does not pre-validate the name; the server returns an error if it is invalid.

### Home node

`--home_node` (optional on `create`, required on `update`) pins which cluster node
holds the namespace's shards. It must be a current storage candidate; when omitted on
`create`, the cluster picks one automatically.

```bash
# Pin a home node at creation time
weaviate-cli create namespace --name tenantswest --home_node node1 --json

# Move future placements to a different node (existing live shards are NOT moved)
weaviate-cli update namespace --name tenantswest --home_node node2 --json
```

`update namespace` is backed by the `PUT /namespaces/{name}` endpoint and only changes
the home node — it is the single mutable field on a namespace.

### State

Namespaces carry a read-only `state` field (`active` or `deleting`). When the server
reports it, `get namespace` surfaces it: a `State:` line in text output and a `"state"`
key in JSON. `home_node` is shown the same way (a `Home node:` line / `"home_node"`
key). Both keys are omitted when the server does not return them (e.g. older clusters).

## Namespace-scoped DB users

On namespace-enabled clusters, a dynamic DB user is bound to a namespace by giving it
a **namespace-qualified id** of the form `<namespace>:<user>`. There is no separate
`--namespace` flag — the namespace is part of the `--user_name` value, which the CLI
forwards verbatim to `client.users.db.create(user_id=...)`. The server derives the
namespace from the qualified id.

```bash
# Create a user inside a namespace (qualified id "<namespace>:<user>")
weaviate-cli create user --user_name tenantswest:scoped-user --json
# Output JSON includes user_name and api_key.

# Inspect — for a global operator, text output adds a "Namespace:" line and JSON adds
# a "namespace" key (the server still returns it in the user response).
weaviate-cli get user --user_name tenantswest:scoped-user --json
weaviate-cli get user --all --json
```

To create a non-namespaced user, pass an unqualified `--user_name` (no colon) — this is
the normal form on clusters where namespaces are not enabled.

## RBAC: manage_namespaces

```bash
# Single namespace
weaviate-cli create role --role_name NsAdmin -p manage_namespaces:tenantswest --json

# Multiple namespaces in a single permission
weaviate-cli create role --role_name MultiNsAdmin -p manage_namespaces:tenantswest,tenantseast --json

# Add or remove a permission on an existing role
weaviate-cli assign permission -p manage_namespaces:tenantssouth --role_name NsAdmin --json
weaviate-cli revoke permission -p manage_namespaces:tenantswest --role_name NsAdmin --json
```

`get role` includes a `Namespaces Permissions` block in text output and a
`permissions.namespaces` array in JSON output.

### Restriction

The wildcard form `manage_namespaces` (no namespace name) is rejected — every grant
must name an explicit namespace. Use a comma-separated list to grant on several at
once:

```bash
-p manage_namespaces:ns1,ns2,ns3
```

## Workflow

1. `create namespace --name <ns>` — provision the namespace.
2. `create role --role_name <r> -p manage_namespaces:<ns>` — grant the management permission.
3. `create user --user_name <ns>:<u>` — create a namespace-scoped DB user (qualified id).
4. `assign role --role_name <r> --user_name <u>` — wire the role to the user.
5. Verify: `get namespace --name <ns>`, `get role --role_name <r>`, `get user --user_name <u>`.
6. Cleanup: `delete user` → `revoke role` (or `delete role`) → `delete namespace`.

## Notes

- The CLI delegates the version check (`>=1.38.0`) to the python client; older
  servers return an error from the client itself.
- Namespaces are independent of multi-tenancy — they sit at the auth/account layer,
  not the collection/tenant data layer.

## Collections and `get collection --namespace`

Collection API targets differ by credential type on namespace-enabled clusters:

- **Namespace-scoped DB user:** use `get collection` without `--namespace` (the CLI
  strips `namespace:` from keys returned by `list_all` when loading details).
- **Global operator:** qualify the collection, e.g. `get collection --collection Movies --namespace myns`, or pass `--collection myns:Movies`. For listing all namespaces at once use **`get collection --list-qualified-keys --json`** (recommended; unquoted `--namespace *` is expanded by the shell to filenames). Equivalent: `get collection --namespace '*' --json`. For one namespace only: `get collection --namespace myns --json`.

See [collections.md](collections.md) for examples.
