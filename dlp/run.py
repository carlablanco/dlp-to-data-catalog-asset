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
    parser.add_argument(
        "--dataset",
        type=str,
        help="The BigQuery dataset to be scanned.")
    parser.add_argument(
        "--table",
        type=str,
        help="""The BigQuery table to be scanned. Optional.
                If None, the entire dataset will be scanned.""")
    parser.add_argument(
        "--instance",
        type=str,
        help="""The instance to be used. Optional.""")
    parser.add_argument(
        "--zone",
        type=str,
        help="""The zone to use, e.g. us-central1-b. Optional.""")
    parser.add_argument(
        "--database",
        type=str,
        help="""The database to use. Optional.""")

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
    preprocess = Preprocessing(
        source = args.source, project=args.project,
        bigquery_args = {"dataset": args.dataset, "table": args.table},
        cloudsql_args = {"instance": args.instance, "zone": args.zone,
                       "database": args.database, "table": args.table})
    tables = preprocess.get_dlp_table_list()
    print(tables)
    inspection = DlpInspection(project_id = args.project,
                               language_code = args.language_code, tables = tables)
    inspection.main()


if __name__ == "__main__":
    arguments = parse_arguments()
    run(arguments)
