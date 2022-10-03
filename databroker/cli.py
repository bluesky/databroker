from typing import List, Optional

import typer

from tiled.utils import import_object
from rich.progress import Progress


cli_app = typer.Typer()
admin_app = typer.Typer()
cli_app.add_typer(
    admin_app,
    name="admin",
    help="Administrative utilities for managing databroker.",
)


@admin_app.command("shape-fixer")
def shape_fix(
    uri: str,
    asset_registry_uri: Optional[str] = None,
    query: List[str] = [],
    patch_resource: Optional[str] = None,
    strict: bool = False,
    dry_run: bool = False,
    limit: Optional[int] = None,
    handler: Optional[List[str]] = typer.Option(
        None, help="Handler given as 'SPEC = import_path'"
    ),
):
    """
    Fix shape metadata in Event Descriptors.
    """
    # Imports are here to avoid making CLI slow.
    import databroker.queries
    from databroker.mongo_normalized import MongoAdapter, discover_handlers

    from .shape_fixer import measure, fix

    if handler is None:
        handler_registry = discover_handlers()
    else:
        handler_registry = {}
        for h in handler:
            if " = " not in h:
                raise ValueError(
                    "Handler must be given as 'SPEC = import_path' (spaces matter)"
                )
            k, _, v = h.partition(" = ")
            handler_registry[k] = import_object(v)
    adapter = MongoAdapter.from_uri(uri, asset_registry_uri=asset_registry_uri)
    mds_database = adapter._metadatastore_db
    asset_database = adapter._asset_registry_db
    if patch_resource is not None:
        patch_resource = import_object(patch_resource)
    if dry_run:
        typer.echo("Dry run!")
    for q in query:
        parsed_query = eval(q, vars(databroker.queries))
        adapter = adapter.search(parsed_query)
    typer.echo(f"Migrating {adapter}")

    items = adapter.items()
    if limit:
        typer.echo(f"Limited to first {limit} BlueskyRuns only")
        items = items[:limit]

    with Progress() as progress:
        task = progress.add_task("Migrating...", total=len(items))
        for uid, run in items:
            try:
                for stream_name, stream in run.items():
                    descriptor = stream.metadata["descriptors"][0]
                    recorded_shapes, measured_shapes = measure(
                        mds_database,
                        asset_database,
                        descriptor,
                        adapter.root_map,
                        handler_registry,
                        patch_resource=patch_resource,
                    )
                    if dry_run:
                        msg = "Dry run"
                    else:
                        msg = "Edited"
                        fix(mds_database, descriptor, measured_shapes)
                    if recorded_shapes != measured_shapes:
                        progress.console.print(
                            f"{msg} {uid} {stream_name}: {recorded_shapes} -> {measured_shapes}"
                        )
            except Exception as exc:
                if strict:
                    raise
                progress.console.print(
                    f"Failed: {uid} {exc!r} (Use --strict for more.)"
                )
            progress.update(task, advance=1)


main = cli_app


if __name__ == "__main__":
    main()
