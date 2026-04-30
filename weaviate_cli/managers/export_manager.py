import json
import click
from typing import Optional
from weaviate.client import WeaviateClient
from weaviate.export.export import (
    ExportFileFormat,
    ExportStorage,
    ExportStatusReturn,
)
from weaviate_cli.defaults import (
    CreateExportCollectionDefaults,
    GetExportCollectionDefaults,
    CancelExportCollectionDefaults,
)


BACKEND_MAP = {
    "filesystem": ExportStorage.FILESYSTEM,
    "s3": ExportStorage.S3,
    "gcs": ExportStorage.GCS,
    "azure": ExportStorage.AZURE,
}

FILE_FORMAT_MAP = {
    "parquet": ExportFileFormat.PARQUET,
}


def _resolve_backend(backend: str) -> ExportStorage:
    backend_enum = BACKEND_MAP.get(backend)
    if backend_enum is None:
        raise click.ClickException(
            f"Unknown backend '{backend}'. Allowed: {', '.join(sorted(BACKEND_MAP))}."
        )
    return backend_enum


def _resolve_file_format(file_format: str) -> ExportFileFormat:
    file_format_enum = FILE_FORMAT_MAP.get(file_format)
    if file_format_enum is None:
        raise click.ClickException(
            f"Unknown file format '{file_format}'. Allowed: {', '.join(sorted(FILE_FORMAT_MAP))}."
        )
    return file_format_enum


class ExportManager:
    def __init__(self, client: WeaviateClient) -> None:
        self.client: WeaviateClient = client

    def create_export(
        self,
        export_id: str = CreateExportCollectionDefaults.export_id,
        backend: str = CreateExportCollectionDefaults.backend,
        file_format: str = CreateExportCollectionDefaults.file_format,
        include: Optional[str] = CreateExportCollectionDefaults.include,
        exclude: Optional[str] = CreateExportCollectionDefaults.exclude,
        wait: bool = CreateExportCollectionDefaults.wait,
        json_output: bool = False,
    ) -> None:
        if include and exclude:
            raise click.ClickException(
                "Cannot specify both --include and --exclude. Use one or the other."
            )

        backend_enum = _resolve_backend(backend)
        file_format_enum = _resolve_file_format(file_format)

        include_collections = (
            [c.strip() for c in include.split(",") if c.strip()] if include else None
        )
        exclude_collections = (
            [c.strip() for c in exclude.split(",") if c.strip()] if exclude else None
        )

        result = self.client.export.create(
            export_id=export_id,
            backend=backend_enum,
            file_format=file_format_enum,
            include_collections=include_collections,
            exclude_collections=exclude_collections,
            wait_for_completion=wait,
        )

        if wait and result and result.status.value != "SUCCESS":
            raise click.ClickException(
                f"Export '{export_id}' finished with status '{result.status.value}'."
            )

        if json_output:
            data = {
                "export_id": result.export_id,
                "backend": result.backend,
                "path": result.path,
                "status": result.status.value,
                "collections": result.collections,
            }
            if result.started_at:
                data["started_at"] = str(result.started_at)
            click.echo(json.dumps(data, indent=2, default=str))
        else:
            click.echo(
                f"Export '{export_id}' created successfully with status '{result.status.value}'."
            )
            if result.collections:
                click.echo(f"Collections: {', '.join(result.collections)}")

    def get_export_status(
        self,
        export_id: str = GetExportCollectionDefaults.export_id,
        backend: str = GetExportCollectionDefaults.backend,
        json_output: bool = False,
    ) -> None:
        backend_enum = _resolve_backend(backend)

        result = self.client.export.get_status(
            export_id=export_id,
            backend=backend_enum,
        )

        self._print_export_status(result, json_output=json_output)

    def cancel_export(
        self,
        export_id: str = CancelExportCollectionDefaults.export_id,
        backend: str = CancelExportCollectionDefaults.backend,
        json_output: bool = False,
    ) -> None:
        backend_enum = _resolve_backend(backend)

        success = self.client.export.cancel(
            export_id=export_id,
            backend=backend_enum,
        )

        if success:
            if json_output:
                click.echo(
                    json.dumps(
                        {
                            "status": "success",
                            "message": f"Export '{export_id}' canceled successfully.",
                        },
                        indent=2,
                    )
                )
            else:
                click.echo(f"Export '{export_id}' canceled successfully.")
        else:
            raise click.ClickException(f"Export '{export_id}' could not be canceled.")

    def _print_export_status(
        self, result: ExportStatusReturn, json_output: bool = False
    ) -> None:
        if json_output:
            data = {
                "export_id": result.export_id,
                "backend": result.backend,
                "path": result.path,
                "status": result.status.value,
                "collections": result.collections,
            }
            if result.started_at:
                data["started_at"] = str(result.started_at)
            if result.error:
                data["error"] = result.error
            if result.took_in_ms is not None:
                data["took_in_ms"] = result.took_in_ms
            if result.shard_status:
                data["shard_status"] = {
                    collection: {
                        shard: {
                            "status": progress.status.value,
                            "objects_exported": progress.objects_exported,
                            **({"error": progress.error} if progress.error else {}),
                            **(
                                {"skip_reason": progress.skip_reason}
                                if progress.skip_reason
                                else {}
                            ),
                        }
                        for shard, progress in shards.items()
                    }
                    for collection, shards in result.shard_status.items()
                }
            click.echo(json.dumps(data, indent=2, default=str))
        else:
            click.echo(f"Export ID: {result.export_id}")
            click.echo(f"Backend: {result.backend}")
            click.echo(f"Path: {result.path}")
            click.echo(f"Status: {result.status.value}")
            if result.collections:
                click.echo(f"Collections: {', '.join(result.collections)}")
            if result.started_at:
                click.echo(f"Started at: {result.started_at}")
            if result.error:
                click.echo(f"Error: {result.error}")
            if result.took_in_ms is not None:
                click.echo(f"Took: {result.took_in_ms}ms")
            if result.shard_status:
                click.echo("Shard Status:")
                for collection, shards in result.shard_status.items():
                    click.echo(f"  {collection}:")
                    for shard, progress in shards.items():
                        status_line = f"    {shard}: {progress.status.value} ({progress.objects_exported} objects)"
                        if progress.error:
                            status_line += f" - Error: {progress.error}"
                        if progress.skip_reason:
                            status_line += f" - Skipped: {progress.skip_reason}"
                        click.echo(status_line)
