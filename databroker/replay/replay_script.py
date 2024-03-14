#!/usr/bin/env python3

import argparse
import sys

from databroker.replay import replay


def log(*args, **kwargs):
    kwargs.setdefault("file", sys.stderr)
    return print(*args, **kwargs)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--zmq", help="0MQ address")
    parser.add_argument(
        "--kafka-bootstrap-servers",
        help="Kafka servers, comma-separated string, e.g. "
        "kafka1.nsls2.bnl.gov:9092,kafka2.nsls2.bnl.gov:9092,kafka3.nsls2.bnl.gov:9092",
    )
    parser.add_argument(
        "--kafka-topic", help="Kafka topic, e.g. bmm.bluesky.runengine.documents"
    )
    parser.add_argument("--kafka-key", help="Kafka key")
    catalog_group = parser.add_argument_group(
        description=("Which catalog should we look for Runs in?")
    ).add_mutually_exclusive_group()
    catalog_group.add_argument("--catalog", help="Databroker catalog name")
    catalog_group.add_argument("--msgpack", help="Directory of msgpack files")
    filter_group = parser.add_argument_group(
        description="Which Runs should we replay? Must specify one of these:"
    ).add_mutually_exclusive_group()
    filter_group.add_argument(
        "--all", action="store_true", help="Replay every Run in this Catalog."
    )
    filter_group.add_argument(
        "-q",
        "--query",
        type=str,
        action="append",
        help=(
            "MongoDB-style query or databroker.queries Query. "
            "Narrow results by chaining multiple queries like "
            "-q \"TimeRange(since='2020')\" "
            "-q \"{'sample': 'Au'}\""
        ),
    )
    filter_group.add_argument(
        "--uids",
        type=argparse.FileType("r"),
        action="append",
        help=("Newline-separated (partial) uids. Lines starting with # are skipped."),
    )
    parser.add_argument(
        "--burst",
        action="store_true",
        help=(
            "Set this to turn off simulated timing and push documents "
            "through as fast as possible."
        ),
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0,
        help=("Add a constant delay between documents."),
    )
    args = parser.parse_args(argv)
    if args.zmq:
        from bluesky.callbacks.zmq import Publisher

        publisher = Publisher(args.zmq)
    elif args.kafka_bootstrap_servers and args.kafka_topics:
        from bluesky_kafka import Publisher

        bootstrap_servers = [s.strip() for s in args.kafka_bootstrap_servers.split(",")]
        publisher = Publisher(args.kafka_topic, bootstrap_servers, args.kafka_key)
    else:
        raise ValueError("Either --zmq or --kafka... parameter is required.")
    if args.msgpack:
        from databroker._drivers.msgpack import BlueskyMsgpackCatalog

        catalog = BlueskyMsgpackCatalog([args.msgpack])
    elif args.catalog:
        import databroker

        catalog = databroker.catalog[args.catalog]
    if args.query or args.all:
        import databroker.queries
        queries = []
        if args.query:
            raw_queries = args.query
            ns = vars(databroker.queries)
            for raw_query in raw_queries:
                # Parse string like "{'scan_id': 123}" to dict.
                try:
                    query = eval(raw_query, ns)
                except Exception:
                    raise ValueError(f"Could not parse query {raw_query}.")
                queries.append(query)
        results = catalog
        for query in queries:
            results = results.search(query)
        if not results:
            print("Query yielded no results. Exiting.")
            # This can be a result of bash mangling the query beacuse the
            # caller forgot to escape the $'s.
            print(
                "If your query had dollar signs in it, remember to "
                "escape them like \\$."
            )
            sys.exit(1)
    elif args.uids:
        # Skip blank lines and commented lines.
        uids = []
        for uid_file in args.uids:
            uids.extend(
                line.strip()
                for line in uid_file.read().splitlines()
                if line and not line.startswith("#")
            )
        if not uids:
            print("Found empty input for --uids. Exiting")
            sys.exit(1)
        results = {uid: catalog[uid] for uid in uids}
    else:
        parser.error(
            "Must specify which Runs to replay via --query ... or "
            "--uids ... or --all."
        )
    log(f"Replaying {len(results)} runs.")
    for uid, run in results.items():
        log(f"Replaying run {run}")
        replay(run.documents(fill="no"), publisher, burst=args.burst, delay=args.delay)
        log(f"Done with run {run}")
    log("Complete. Exiting.")


if __name__ == "__main__":
    main()
