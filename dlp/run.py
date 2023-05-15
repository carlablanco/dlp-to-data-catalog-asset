# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a dataset and tags the results in Data Catalog."""

import argparse
from typing import Type

from dlp.preprocess import Preprocessing
from dlp.inspection import DlpInspection


def parse_arguments() -> Type[argparse.Namespace]:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="source")

    bigquery_parser = subparsers.add_parser(
        "bigquery",
        help="Use BigQuery as the data source.")
    bigquery_parser.add_argument(
        "--table",
        type=str,
        help="The BigQuery table to be scanned. Optional.")
    bigquery_parser.add_argument(
        "--dataset",
        required=True,
        type=str,
        help="The BigQuery dataset to be scanned.")

    cloudsql_parser = subparsers.add_parser(
        "cloudsql",
        help="Use CloudSQL as the data source.")
    cloudsql_parser.add_argument(
        "--db_type",
        required=True,
        type=str,
        choices=["postgres","mysql"],
        help="""The CloudSQL type to be scanned.
        e.g. postgres, mysql.""")
    cloudsql_parser.add_argument(
        "--table",
        required=True,
        type=str,
        help="The CloudSQL table to be scanned.")
    cloudsql_parser.add_argument(
        "--instance",
        required=True,
        type=str,
        help="The name of the database instance used.")
    cloudsql_parser.add_argument(
        "--zone",
        required=True,
        type=str,
        help="The zone to use, e.g. us-central1-b.")
    cloudsql_parser.add_argument(
        "--db_user",
        required=True,
        type=str,
        help="The database user that matches the default gcloud user")
    cloudsql_parser.add_argument(
        "--db_name",
        required=True,
        type=str,
        help="The database to use. e.g. Bigquery, CloudSQL.")

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

    return parser.parse_args()


def run(args: Type[argparse.Namespace]):
    """Runs DLP inspection scan and tags the results to Data Catalog.

    Args:
        source(str): The source of data used.
        project(str): Project ID for which the client acts on behalf of.
        language_code(str): The BCP-47 language code to use, e.g. 'en-US'.
        dataset(str): The BigQuery dataset to be scanned. Optional.
        table(str): The name of the table. Optional.
        db_typestr): Type of the database. e.g. postgres, mysql. Optional.
        db_user(str): Default gcloud user's matching database user. Optional.
        instance(str): Name of the database instance. Optional.
        zone(str): The name of the zone. Optional.
        db_name(str): The name of the database. Optional.
    """
    source = args.source
    project = args.project
    language_code = args.language_code

    preprocess_args = {}
    if source == "bigquery":
        preprocess_args = {
            "bigquery_args": {
                "dataset": args.dataset,
                "table": args.table
            }
        }
    elif source == "cloudsql":
        preprocess_args = {
            "cloudsql_args": {
                "instance": args.instance,
                "zone": args.zone,
                "db_user": args.db_user,
                "db_name": args.db_name,
                "table": args.table,
                "db_type": args.db_type
            }
        }
    else:
        # Handle unsupported source
        raise ValueError("Unsupported source: " + source)

    preprocess = Preprocessing(
        source=source, project=project, **preprocess_args)
    tables = preprocess.get_dlp_table_list()
    inspection = DlpInspection(project_id=project,
                               language_code=language_code,
                               tables=tables)
    inspection.main()

if __name__ == "__main__":
    arguments = parse_arguments()
    run(arguments)
