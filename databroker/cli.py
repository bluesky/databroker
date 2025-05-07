import json
from typing import List, Optional

import tiled.client
import tiled.server.app
import typer
from rich.progress import Progress
from tiled.utils import import_object
from typing_extensions import Annotated

cli_app = typer.Typer()
admin_app = typer.Typer()
cli_app.add_typer(
    admin_app,
    name="admin",
    help="Administrative utilities for managing databroker.",
)


def parse_dict_arg(arg):
    """
    Parse a string like '{"key1": "value1", "key2": 2}' into a dictionary.

    Returns:
        dict: The parsed dictionary or an empty dict if arg is None or empty.
    """
    if arg is None or arg.strip() == '':
        return {}

    try:
        result = json.loads(arg)
        if isinstance(result, dict):
            return result
        else:
            raise ValueError("Parsed value is not a dictionary.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON string: {arg}") from e


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
    root_map: Annotated[Optional[dict], typer.Option(parser=parse_dict_arg)] = None,
):
    """
    Fix shape metadata in Event Descriptors.
    """
    # Imports are here to avoid making CLI slow.
    import databroker.queries
    from databroker.mongo_normalized import MongoAdapter, discover_handlers

    from .shape_fixer import measure, patch

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

    root_map = root_map if root_map is not None else {}
    root_map = getattr(adapter, "root_map", {}) | root_map
    typer.echo(f"Using root map: {root_map}")

    app = tiled.server.app.build_app(adapter)
    with tiled.client.Context.from_app(app) as context:
        tiled_client = tiled.client.from_context(context)

        with Progress() as progress:
            task = progress.add_task("Migrating...", total=len(items))
            for uid, run in items:
                try:
                    for stream_name, stream in run.items():
                        descriptor = stream.metadata()["descriptors"][0]
                        recorded_shapes, measured_shapes = measure(
                            mds_database,
                            asset_database,
                            descriptor,
                            root_map,
                            handler_registry,
                            patch_resource=patch_resource,
                        )
                        if dry_run:
                            msg = "Dry run"
                        else:
                            msg = "Edited"
                            patch(tiled_client[uid][stream_name], measured_shapes)
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
