import semver
import click
from typing import Optional
from weaviate.backup.backup import BackupConfigCreate
from weaviate.client import WeaviateClient
from weaviate_cli.defaults import (
    CancelBackupDefaults,
    CreateBackupDefaults,
    RestoreBackupDefaults,
    GetBackupDefaults,
)


class BackupManager:
    def __init__(self, client: WeaviateClient) -> None:
        self.client = client

    def create_backup(
        self,
        backup_id: str = CreateBackupDefaults.backup_id,
        backend: str = CreateBackupDefaults.backend,
        include: Optional[str] = CreateBackupDefaults.include,
        exclude: Optional[str] = CreateBackupDefaults.exclude,
        wait: bool = CreateBackupDefaults.wait,
        cpu_for_backup: int = CreateBackupDefaults.cpu_for_backup,
    ) -> None:

        version = semver.Version.parse(self.client.get_meta()["version"])
        if include:
            for collection in include.split(","):
                if not self.client.collections.exists(collection):
                    raise Exception(
                        f"Collection '{collection}' does not exist in Weaviate. Cannot include in backup."
                    )
        if exclude:
            for collection in exclude.split(","):
                if not self.client.collections.exists(collection):
                    raise Exception(
                        f"Collection '{collection}' does not exist in Weaviate. Cannot exclude from backup."
                    )

        result = self.client.backup.create(
            backup_id=backup_id,
            backend=backend,
            include_collections=include.split(",") if include else None,
            exclude_collections=exclude.split(",") if exclude else None,
            wait_for_completion=wait,
            config=(
                BackupConfigCreate(
                    cpu_percentage=cpu_for_backup,
                )
                if version.compare(semver.Version.parse("1.25.0")) > 0
                else None
            ),
        )

        if wait and result and result.status.value != "SUCCESS":
            raise Exception(
                f"Backup '{backup_id}' failed with status '{result.status.value}'"
            )

        click.echo(f"Backup '{backup_id}' created successfully in Weaviate.")

    def restore_backup(
        self,
        backup_id: str = RestoreBackupDefaults.backup_id,
        backend: str = RestoreBackupDefaults.backend,
        include: Optional[str] = RestoreBackupDefaults.include,
        exclude: Optional[str] = RestoreBackupDefaults.exclude,
        wait: bool = RestoreBackupDefaults.wait,
    ) -> None:

        result = self.client.backup.restore(
            backup_id=backup_id,
            backend=backend,
            include_collections=include.split(",") if include else None,
            exclude_collections=exclude.split(",") if exclude else None,
            wait_for_completion=wait,
        )

        if wait and result and result.status.value != "SUCCESS":

            raise Exception(
                f"Backup '{backup_id}' failed with status '{result.status.value}'"
            )

        click.echo(f"Backup '{backup_id}' restored successfully in Weaviate.")

    def get_backup(
        self,
        backend: str = GetBackupDefaults.backend,
        backup_id: str = GetBackupDefaults.backup_id,
        restore: bool = GetBackupDefaults.restore,
    ) -> None:

        if restore:
            backup = self.client.backup.get_restore_status(
                backup_id=backup_id,
                backend=backend,
            )
            print(f"Backup ID: {backup.backup_id}")
            print(f"Backup Path: {backup.path}")
            print(f"Backup Status: {backup.status}")
            if "collections" in backup:
                print(f"Collections: {backup.collections}")
        else:
            if backup_id is not None:
                backup = self.client.backup.get_create_status(
                    backup_id=backup_id, backend=backend
                )
                print(f"Backup ID: {backup.backup_id}")
                print(f"Backup Path: {backup.path}")
                print(f"Backup Status: {backup.status}")
                if "collections" in backup:
                    print(f"Collections: {backup.collections}")
            else:
                raise Exception("This functionality is not supported yet.")
                # backups = client.backup.list_backups(backend=backend)
                # for backup in backups:
                #     print(f"Backup ID: {backup.backup_id}")
                #     print(f"Backup Path: {backup.path}")
                #     print(f"Backup Status: {backup.status}")
                #     if "collections" in backup:
                #       print(f"Collections: {backup.collections}")
                #     print("------------------------------")

    def cancel_backup(
        self,
        backend: str = CancelBackupDefaults.backend,
        backup_id: str = CancelBackupDefaults.backup_id,
    ) -> None:
        if self.client.backup.cancel(backend=backend, backup_id=backup_id):
            click.echo(f"Backup '{backup_id}' cancelled successfully in Weaviate.")
        else:
            backup_status = self.client.backup.get_create_status(
                backend=backend, backup_id=backup_id
            )
            raise Exception(
                f"Error: Backup '{backup_id}' could not be cancelled in Weaviate. Current status: {backup_status.status}"
            )
