import semver
import click

from weaviate.backup.backup import BackupConfigCreate
from weaviate.client import WeaviateClient


class BackupManager:
    def __init__(self, client: WeaviateClient) -> None:
        self.client = client

    def create_backup(
        self,
        backup_id: str,
        backend,
        include: str,
        exclude: str,
        wait: bool,
        cpu_for_backup: int,
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
        self, backup_id: str, backend, include: str, exclude: str, wait: bool
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
