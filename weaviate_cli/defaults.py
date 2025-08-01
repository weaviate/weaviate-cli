from dataclasses import dataclass, field
from typing import Optional, List, Dict


PERMISSION_HELP_STRING = (
    "Permission in format action:collection. Can be specified multiple times.\n\n"
    "Available Permissions:\n\n"
    "  Role management: create_roles, read_roles, update_roles, delete_roles\n\n"
    "  Cluster management: read_cluster\n\n"
    "  Backup management: manage_backups\n\n"
    "  Collection management: create_collections, read_collections, update_collections, delete_collections\n\n"
    "  Tenant management: create_tenants, read_tenants, update_tenants, delete_tenants\n\n"
    "  Data management: create_data, read_data, update_data, delete_data\n\n"
    "  User management: assign_and_revoke_users, read_users\n\n"
    "  Node management: read_nodes\n\n"
    "  Alias management: create_aliases, read_aliases, update_aliases, delete_aliases\n\n"
    "  CRUD shorthands for collections, roles, tenants, users and data:\n\n"
    "    crud_collections:collection,cud_data:collection,rd_collections,\n\n"
    "    crud_roles:role,cud_tenants:tenant,rd_tenants,\n\n"
    "    crud_users:user,cud_data:*,rd_data:*\n\n"
    "    crud_aliases:Movies,cud_aliases:Movies,rd_aliases:Movies\n\n"
    "Subfields:\n\n"
    "  - *_collections:collection name, can be specified multiple times\n\n"
    "  - *_roles:role name:scope, can be specified multiple times.\n\n"
    "  - *_users:user_name, can be specified multiple times\n\n"
    "  - *_tenants:collection_name:tenant_name, can be specified multiple times\n\n"
    "  - *_aliases:collection_name:alias_name, can be specified multiple times\n\n"
    "  - *_data:collection_name, can be specified multiple times\n\n"
    "  - *_backups:collection_name, can be specified multiple times\n\n"
    "  - read_nodes:verbosity (verbosity level)\n\n"
    "Examples:\n\n"
    "  --permission crud_collections:Movies\n\n"
    "  --permission cud_tenants:Person_*\n\n"
    "  --permission rucd_data:Person_*\n\n"
    "  --permission assign_and_revoke_users:user-1,user-2\n\n"
    "  --permission create_collections:Movies,Books\n\n"
    "  --permission create_tenants:Movies,Books\n\n"
    "  --permission crud_tenants:Movies,Books:MyTenant*,YourTenant*\n\n"
    "  --permission crud_roles:Admin,Editor:all\n\n"
    "  --permission create_roles:Editor:match\n\n"
    "  --permission read_roles:Admin,Editor:match\n\n"
    "  --permission read_nodes:verbose:Movies\n\n"
    "  --permission read_nodes:minimal\n\n"
    "  --permission manage_backups:Movies\n\n"
    "  --permission read_cluster\n\n"
    "  --permission crud_aliases:Movies,Books:Alias*\n\n"
    "  --permission create_aliases:Banks\n\n"
    "  --permission crud_users:user-1,user-2\n\n"
)
QUERY_MAXIMUM_RESULTS = 10000
MAX_OBJECTS_PER_BATCH = 5000


@dataclass
class CreateCollectionDefaults:
    collection: str = "Movies"
    replication_factor: int = 3
    async_enabled: bool = False
    vector_index: str = "hnsw"
    inverted_index: Optional[str] = None
    training_limit: int = 10000
    multitenant: bool = False
    auto_tenant_creation: bool = False
    auto_tenant_activation: bool = False
    force_auto_schema: bool = False
    shards: int = 0
    vectorizer: str = "none"
    vectorizer_base_url: Optional[str] = None
    replication_deletion_strategy: str = "no_automated_resolution"
    named_vector: bool = False
    named_vector_name: Optional[str] = "default"


@dataclass
class CreateTenantsDefaults:
    collection: str = "Movies"
    tenant_suffix: str = "Tenant"
    number_tenants: int = 100
    tenant_batch_size: Optional[int] = None
    state: str = "active"


@dataclass
class CreateBackupDefaults:
    backend: str = "s3"
    backup_id: str = "test-backup"
    include: Optional[str] = None
    exclude: Optional[str] = None
    wait: bool = False
    cpu_for_backup: int = 40


@dataclass
class CreateDataDefaults:
    collection: str = "Movies"
    limit: int = 1000
    consistency_level: str = "quorum"
    randomize: bool = False
    auto_tenants: int = 0
    vector_dimensions: int = 1536
    skip_seed: bool = False
    wait_for_indexing: bool = False
    verbose: bool = False
    multi_vector: bool = False


@dataclass
class CreateRoleDefaults:
    role_name: str = "NewRole"
    permission: tuple = ()


@dataclass
class CancelBackupDefaults:
    backend: str = "s3"
    backup_id: str = "test-backup"


@dataclass
class DeleteCollectionDefaults:
    collection: str = "Movies"
    all: bool = False


@dataclass
class DeleteTenantsDefaults:
    collection: str = "Movies"
    tenant_suffix: str = "Tenant"
    number_tenants: int = 100
    tenants: Optional[list] = None


@dataclass
class DeleteDataDefaults:
    collection: str = "Movies"
    limit: int = 100
    consistency_level: str = "quorum"
    uuid: Optional[str] = None
    verbose: bool = False


@dataclass
class DeleteRoleDefaults:
    role_name: str = "NewRole"


@dataclass
class GetCollectionDefaults:
    collection: Optional[str] = None


@dataclass
class GetTenantsDefaults:
    collection: str = "Movies"
    tenant_id: Optional[str] = None
    verbose: bool = False


@dataclass
class GetShardsDefaults:
    collection: Optional[str] = None


@dataclass
class GetRoleDefaults:
    role_name: Optional[str] = None
    user_name: Optional[str] = None
    user_type: Optional[str] = "db"


@dataclass
class GetBackupDefaults:
    backend: str = "s3"
    backup_id: Optional[str] = None
    restore: bool = False


@dataclass
class GetUserDefaults:
    role_name: Optional[str] = None
    user_name: Optional[str] = None


@dataclass
class QueryDataDefaults:
    collection: str = "Movies"
    search_type: str = "fetch"
    query: str = "Action movie"
    consistency_level: str = "quorum"
    limit: int = 10
    properties: str = "title,keywords"
    tenants: Optional[str] = None
    target_vector: Optional[str] = None


@dataclass
class RestoreBackupDefaults:
    backend: str = "s3"
    backup_id: str = "test-backup"
    wait: bool = False
    include: Optional[str] = None
    exclude: Optional[str] = None


@dataclass
class UpdateCollectionDefaults:
    collection: str = "Movies"
    async_enabled: Optional[bool] = None
    replication_factor: Optional[int] = None
    vector_index: Optional[str] = None
    description: Optional[str] = None
    training_limit: int = 10000
    auto_tenant_creation: Optional[bool] = None
    auto_tenant_activation: Optional[bool] = None
    replication_deletion_strategy: Optional[str] = None


@dataclass
class UpdateTenantsDefaults:
    collection: str = "Movies"
    tenant_suffix: str = "Tenant"
    number_tenants: int = 100
    state: str = "active"
    tenants: Optional[str] = None


@dataclass
class UpdateShardsDefaults:
    collection: Optional[str] = None
    status: str = "READY"
    shards: Optional[str] = None
    all: bool = False


@dataclass
class UpdateDataDefaults:
    collection: str = "Movies"
    limit: int = 100
    consistency_level: str = "quorum"
    randomize: bool = False
    skip_seed: bool = False
    verbose: bool = False


@dataclass
class UpdateUserDefaults:
    user_name: Optional[str] = None
    rotate_api_key: bool = False
    activate: bool = False
    deactivate: bool = False


@dataclass
class GetNodesDefaults:
    collection: Optional[str] = None


@dataclass
class GetAliasDefaults:
    alias_name: Optional[str] = None
    collection: Optional[str] = None
    all: bool = False
