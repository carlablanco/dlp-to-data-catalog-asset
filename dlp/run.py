# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a dataset and tags the results in Data Catalog."""

import argparse
from typing import Type
from dlp.preprocess import Preprocessing, Source
from dlp.inspection import DlpInspection


def parse_arguments() -> Type[argparse.Namespace]:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        type=Source,
        choices=Source,
        metavar="SOURCE[bigquery, cloudsql-postgres, cloudsql-mysql]",
        required=True,
        help="""The source of data used, e.g. 'cloudsql-postgres'
                'cloudsql-mysql' 'bigquery'.""")
    parser.add_argument(
        "--project",
        type=str,
        required=True,
        help="The Google Cloud project to be used.")
    parser.add_argument(
        "--language_code",
        type=str,
        required=True,
        help="The BCP-47 language code to use, e.g. 'en-US'.")

    main_args, _ = parser.parse_known_args()

    if main_args.source == Source.BIGQUERY:
        bigquery_group = parser.add_argument_group("Bigquery Group")
        bigquery_group.add_argument(
            "--table",
            type=str,
            help="""The BigQuery table to be scanned. Optional.
                    If None, the entire dataset will be scanned.""")
        bigquery_group.add_argument(
            "--dataset",
            required=True,
            type=str,
            help="The BigQuery dataset to be scanned.")

    if main_args.source in [Source.MYSQL, Source.POSTGRES]:
        cloudsql_group = parser.add_argument_group("CloudSQL Group")
        cloudsql_group.add_argument(
            "--table",
            required=True,
            type=str,
            help="The CloudSQL table to be scanned.")
        cloudsql_group.add_argument(
            "--instance",
            required=True,
            type=str,
            help="The instance to be used. Optional.")
        cloudsql_group.add_argument(
            "--zone",
            required=True,
            type=str,
            help="The zone to use, e.g. us-central1-b. Optional.")
        cloudsql_group.add_argument(
            "--database",
            required=True,
            type=str,
            help="The database to use. Optional.")

    return parser.parse_args()


def run(args: Type[argparse.Namespace]):
    """Runs DLP inspection scan and tags the results to Data Catalog.

    Args:
        project: Project ID for which the client acts on behalf of.
        language_code: The BCP-47 language code to use, e.g. 'en-US'.
        dataset: The BigQuery dataset to be scanned.
        table: The BigQuery table to be scanned. Optional.
                If None, the entire dataset will be scanned.
    """
    source = args.source
    project = args.project
    language_code = args.language_code
    dataset = args.dataset if hasattr(args, 'dataset') else None
    table = args.table if hasattr(args, 'table') else None
    instance = args.instance if hasattr(args, 'instance') else None
    zone = args.zone if hasattr(args, 'zone') else None
    database = args.database if hasattr(args, 'database') else None

    preprocess = Preprocessing(
        source=source, project=project,
        bigquery_args={"dataset": dataset, "table": table},
        cloudsql_args={"instance": instance, "zone": zone,
                       "database": database, "table": table})
    tables = preprocess.get_dlp_table_list()
    inspection = DlpInspection(project_id=project,
                               language_code=language_code, tables=tables)
    inspection.main()


if __name__ == "__main__":
    arguments = parse_arguments()
    run(arguments)
