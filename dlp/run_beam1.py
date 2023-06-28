# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a dataset and tags the results in Data Catalog."""

import argparse
import re
from typing import Type

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
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
        "--zone",
        required=True,
        type=str,
        help="The zone to use, e.g. us-central1-b.",
    )

    return parser.parse_args()


def run(args: Type[argparse.Namespace]):
    """Runs DLP inspection scan and tags the results to Data Catalog.

    Args:

        source (str): The name of the source of data used.
        project (str): The name of the Google Cloud Platform project.
        location_category (str): The location to be inspected. Ex. "CANADA".
        zone (str): The default location to use when making API calls..
        bigquery_args (Dict):
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
    zone = args.zone

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project='data-poc-sandbox-378818',
        region='us-central1',
        staging_location='gs://jimedina-bucket-prueba/stagingrun4',
        temp_location='gs://jimedina-bucket-prueba/temprun4',
        template_location='gs://jimedina-bucket-prueba/templateDataflow4',
        setup_file='./setup.py',
        save_main_session=True
    )

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
                "service_account": args.service_account,
                "db_name": dataset,
                "table": table,
                "db_type": args.db_type,
            }
        }
    else:
        # Handle unsupported source
        raise ValueError("Unsupported source: " + source)

    # Create preprocessing and DLP inspection objects
    def process_table(table_tuple):
        table_name, start_index = table_tuple
        preprocess = Preprocessing(
            source=source,
            project=project,
            zone = zone, 
            **preprocess_args)

        dlp_table = preprocess.get_dlp_table_per_block(50000, table_name, start_index)
        return table_name,dlp_table

    def inspect_table(table_tuple):
        table_name,dlp_table = table_tuple
        dlpinspection = DlpInspection(project_id=project,
                    location_category=location_category)

        finding_results_per_block = dlpinspection.get_finding_results(dlp_table)
        return table_name,finding_results_per_block

    def get_tables_info():
        preprocess = Preprocessing(
                source=source, project=project, **preprocess_args)
        tables_info = preprocess.get_tables_info()

        combined_list = []

        for table_name,total_cells in tables_info:
            range_list = list(range(0,total_cells,50000))
            for num in range_list:
                combined_list.append((table_name,num))
        return combined_list

    def merge_and_top_finding(finding_tuple):
        table_name,finding_results = finding_tuple

        dlpinspection = DlpInspection(project_id=project,
                location_category=location_category)
        top_finding = dlpinspection.merge_finding_results(finding_results)
        print(top_finding)
        return table_name,top_finding

    def process_catalog(top_finding_tuple):
        table_name,top_finding = top_finding_tuple

        catalog = Catalog(
            data=top_finding,
            project_id=project,
            zone=zone,
            dataset=dataset,
            table=table_name,
            instance_id=instance_id,
        )
        catalog.main()


    with beam.Pipeline() as p:

        (p | f'Get_tables_info' >> beam.Create(get_tables_info())
           | f'PreProcessTable' >> beam.Map(process_table)
           | f'Inspect' >> beam.Map(inspect_table)
           | f'GroupByKey' >> beam.GroupByKey()
           | f'ProcessTopFinding' >> beam.Map(merge_and_top_finding)
           | f'ProcessCatalog' >> beam.Map(process_catalog)

        )


if __name__ == "__main__":
    arguments = parse_arguments()
    run(arguments)
