from dataclasses import dataclass, field
from typing import Optional, List, Dict


PERMISSION_HELP_STRING = (
    "Permission in format action:collection. Can be specified multiple times.\n\n"
    "Available Permissions:\n\n"
    "  Role management: manage_roles, read_roles\n\n"
    "  Cluster management: manage_cluster, read_cluster\n\n"
    "  Backup management: manage_backups\n\n"
    "  Schema management: create_schema, read_schema, update_schema, delete_schema\n\n"
    "  Data management: create_data, read_data, update_data, delete_data\n\n"
    "  Node management: read_nodes\n\n"
    "  CRUD shorthands for collections and data:\n\n"
    "    crud_collections:collection,cud_data:collection,dur_data:*,rd_collections\n\n"
    "Examples:\n\n"
    "  --permission crud_collections:Movies\n\n"
    "  --permission rucd_data:Person_*\n\n"
    '  --permission "create_collections:Movies,Books"\n\n'
    '  --permission "manage_roles:Admin,Editor"\n\n'
    "  --permission read_nodes:verbose:Movies\n\n"
    "  --permission manage_backups:Movies\n\n"
    "  --permission read_cluster"
)


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
    shards: int = 1
    vectorizer: Optional[str] = None
    replication_deletion_strategy: str = "delete_on_conflict"


@dataclass
class CreateTenantsDefaults:
    collection: str = "Movies"
    tenant_suffix: str = "Tenant--"
    number_tenants: int = 100
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
    tenant_suffix: str = "Tenant--"
    number_tenants: int = 100


@dataclass
class DeleteDataDefaults:
    collection: str = "Movies"
    limit: int = 100
    consistency_level: str = "quorum"
    uuid: Optional[str] = None


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


@dataclass
class GetBackupDefaults:
    backend: str = "s3"
    backup_id: Optional[str] = None
    restore: bool = False


@dataclass
class GetUserDefaults:
    role_name: Optional[str] = None


@dataclass
class QueryDataDefaults:
    collection: str = "Movies"
    search_type: str = "fetch"
    query: str = "Action movie"
    consistency_level: str = "quorum"
    limit: int = 10
    properties: str = "title,keywords"


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
    vector_index: Optional[str] = None
    description: Optional[str] = None
    training_limit: int = 10000
    auto_tenant_creation: Optional[bool] = None
    auto_tenant_activation: Optional[bool] = None
    replication_deletion_strategy: Optional[str] = None


@dataclass
class UpdateTenantsDefaults:
    collection: str = "Movies"
    tenant_suffix: str = "Tenant--"
    number_tenants: int = 100
    state: str = "active"


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


@dataclass
class GetNodesDefaults:
    collection: Optional[str] = None
