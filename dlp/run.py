# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a dataset and tags the results in Data Catalog."""

import argparse
import re
from typing import Type

from dlp.preprocess import Preprocessing
from dlp.inspection import DlpInspection
from dlp.catalog import Catalog


EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")


def is_valid_email(email: str) -> bool:
    """Checks if a string is a valid email."""
    return bool(EMAIL_REGEX.match(email))


def email_type(value) -> str:
    """Validates and returns a valid email."""
    if not is_valid_email(value):
        raise argparse.ArgumentTypeError(f"Invalid IAM user: {value}")
    return value


def parse_arguments() -> Type[argparse.Namespace]:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="source")

    bigquery_parser = subparsers.add_parser(
        "bigquery",
        help="Use BigQuery as the data source."
    )
    bigquery_parser.add_argument(
        "--table",
        type=str,
        help="The BigQuery table to be scanned. Optional.",
    )
    bigquery_parser.add_argument(
        "--dataset",
        required=True,
        type=str,
        help="The BigQuery dataset to be scanned.",
    )

    cloudsql_parser = subparsers.add_parser(
        "cloudsql",
        help="Use CloudSQL as the data source.",
    )
    cloudsql_parser.add_argument(
        "--db_type",
        required=True,
        type=str,
        choices=["postgres", "mysql"],
        help="The CloudSQL type to be scanned. e.g. postgres, mysql.",
    )
    cloudsql_parser.add_argument(
        "--table",
        required=True,
        type=str,
        help="The CloudSQL table to be scanned.",
    )
    cloudsql_parser.add_argument(
        "--instance",
        required=True,
        type=str,
        help="The name of the database instance used.",
    )
    cloudsql_parser.add_argument(
        "--zone",
        required=True,
        type=str,
        help="The zone to use, e.g. us-central1-b.",
    )
    cloudsql_parser.add_argument(
        "--service_account",
        required=True,
        type=email_type,
        metavar="service_account",
        help="Email address of the service account to be used.",
    )
    cloudsql_parser.add_argument(
        "--db_name",
        required=True,
        type=str,
        help="The database to use. e.g. Bigquery, CloudSQL.",
    )

    # Common arguments.
    parser.add_argument(
        "--project",
        type=str,
        required=True,
        help="The Google Cloud project to be used.",
    )
    parser.add_argument(
        "--location_category",
        type=str,
        required=True,
        help="The location to be inspected. Ex. 'CANADA'",
    )
    parser.add_argument(
        "--location",
        type=str,
        required=True,
        help="The location of the engine'.",
    )

    return parser.parse_args()


def run(args: Type[argparse.Namespace]):
    """Runs DLP inspection scan and tags the results to Data Catalog.

    Args:

        source (str): The name of the source of data used.
        project (str): The name of the Google Cloud Platform project.
        location_category (str): The location to be inspected. Ex. "CANADA".
        location(str): The compute engine region.
        bigquery_args(Dict):
            dataset (str): The name of the BigQuery dataset.
            table (str, optional): The name of the BigQuery table. If not
              provided, the entire dataset is scanned. Optional.
              Defaults to None.
        cloudsql_args(Dict):
            instance (str): Name of the database instance.
            zone(str): The name of the zone.
            service_account(str): Service account email to be used.
            db_name(str): The name of the database.
            table (str): The name of the table.
            db_type(str): The type of the database. e.g. postgres, mysql.
    """

    source = args.source
    project = args.project
    location_category = args.location_category
    location = args.location

    if source == "bigquery":
        dataset = args.dataset
        table = args.table
        preprocess_args = {
            "bigquery_args": {"dataset": dataset, "table": table}
        }
        instance_id = None
    elif source == "cloudsql":
        instance_id = args.instance
        dataset = args.db_name
        table = args.table
        preprocess_args = {
            "cloudsql_args": {
                "instance": instance_id,
                "zone": args.zone,
                "service_account": args.service_account,
                "db_name": dataset,
                "table": table,
                "db_type": args.db_type,
            }
        }
    else:
        # Handle unsupported source
        raise ValueError("Unsupported source: " + source)

    # Specify the number of cells to analyze per batch.
    cells_to_analyze = 200000

    # Create preprocessing and DLP inspection objects
    preprocess = Preprocessing(
        source=source,
        project=project,
        **preprocess_args,
    )

    dlpinspection = DlpInspection(project_id=project,
                location_category=location_category)

    # Get a list of table names.
    table_names = preprocess.get_table_names()
    # Store the top finding for each table.
    top_finding_tables = []

    # Iterate through each table to obtain the finding_result_per_table.
    for table_name in table_names:
        finding_results_per_table = []
        empty_search = False
        start_index = 0
        while not empty_search:
            # Retrieve DLP table per batch of cells.
            dlp_table = preprocess.get_dlp_table_per_block(
                cells_to_analyze,table_name,start_index)
            finding_result_per_block = dlpinspection.get_finding_results(
                dlp_table)
            finding_results_per_table.append(finding_result_per_block)

            if not dlp_table.rows:
                empty_search = True
            start_index += cells_to_analyze

        # Obtain the top finding for the table.
        top_finding_per_table = dlpinspection.merge_and_top_finding(
            finding_results_per_table)
        # Add the table and its top_finding to the list.
        top_finding_tables.append(top_finding_per_table)

    print(top_finding_tables)

    if source == "bigquery" and table is None:
        # If scanning entire dataset.
        bigquery_tables = preprocess.get_bigquery_tables(dataset)
        for i, table in enumerate(bigquery_tables):
            catalog = Catalog(
                data=top_finding_tables[i],
                project_id=project,
                location=location,
                dataset=dataset,
                table=table,
                instance_id=instance_id,
            )
            catalog.main()
    else:
        # If scanning a specific table.
        catalog = Catalog(
            data=top_finding_tables[0],
            project_id=project,
            location=location,
            dataset=dataset,
            table=table,
            instance_id=instance_id,
        )
        catalog.main()


if __name__ == "__main__":
    arguments = parse_arguments()
    run(arguments)
