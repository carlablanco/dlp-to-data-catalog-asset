# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a dataset and tags the results in Data Catalog."""

import argparse
from typing import Type, List, Tuple, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from dlp.preprocess import Preprocessing
from dlp.inspection import DlpInspection
from dlp.catalog import Catalog
import dlp.run



def parse_arguments() -> Type[argparse.ArgumentParser]:
    """Parses command line arguments."""

    parser = dlp.run.parse_arguments()
    parser.add_argument(
        "--temp_file_location",
        type=str,
        required=True,
        help="""Specifies the location in Google Cloud Storage where
        temporary files will be stored during the dataflow execution.""",
    )
    parser.add_argument(
        "--staging_location",
        type=str,
        required=True,
        help="""Specifies the location in Google Cloud Storage where files
        will be staged during the dataflow execution.""",
    )
    parser.add_argument(
        "--template_location",
        type=str,
        required=True,
        help="""Specifies the location in Google Cloud Storage where the
        dataflow template will be stored.""",
    )
    parser.add_argument(
        "--output_txt_location",
        type=str,
        required=True,
        help="Specifies the location where the output text will be stored.",
    )

    return parser


def run(args: Type[argparse.Namespace]):
    """Runs DLP inspection scan and tags the results to Data Catalog.

    Args:

        source (str): The name of the source of data used.
        project (str): The name of the Google Cloud Platform project.
        location_category (str): The location to be inspected. Ex. "CANADA".
        zone(str): The name of the zone.
        bigquery_args(Dict):
            dataset (str): The name of the BigQuery dataset.
            table (str, optional): The name of the BigQuery table. If not
              provided, the entire dataset is scanned. Optional.
              Defaults to None.
        cloudsql_args(Dict):
            instance (str): Name of the database instance.
            service_account(str): Service account email to be used.
            db_name(str): The name of the database.
            table (str): The name of the table.
            db_type(str): The type of the database. e.g. postgres, mysql.
    """

    source = args.source
    project = args.project
    location_category = args.location_category
    zone = args.zone
    temp_file_location = args.temp_file_location
    staging_location = args.staging_location
    template_location = args.template_location
    output_txt_location = args.output_txt_location

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=project,
        region=zone,
        staging_location=staging_location,
        temp_file_location=temp_file_location,
        template_location=template_location,
        setup_file='./setup.py',
        save_main_session=True,
    )

    if source == "bigquery":
        dataset = args.dataset
        table = args.table
        entry_group_name = None
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
        catalog = Catalog(
            data=None,
            project_id=project,
            zone=zone,
            instance_id=instance_id,
            entry_group_name=None,
        )
        entry_group_name = catalog.create_custom_entry_group()
    else:
        # Handle unsupported source
        raise ValueError("Unsupported source: " + source)

    def get_tables_info() -> List[Tuple]:
        """Retrieve information about tables
        and their corresponding start indexes.

        Returns:
            List[Tuple]: A list of tuples containing
            the table name and start index.
        """
        preprocess = Preprocessing(
            source=source, project=project, zone=zone, **preprocess_args)
        tables_info = preprocess.get_tables_info()
        tables_start_index_list = []

        for table_name, total_cells in tables_info:
            range_list = list(range(0, total_cells, 50000))
            for num in range_list:
                tables_start_index_list.append((table_name, num))
        return tables_start_index_list

    def preprocess_table(table_start_index_tuple: Tuple) -> Tuple:
        """Process table based on their start indexes and retrieve DLP tables.

        Args:
            table_start_index (Tuple): Tuple containing
            the table name and start index.

        Returns:
            Tuple: Tuple containing the table name and DLP table objects.

        """
        table_name, start_index = table_start_index_tuple
        preprocess = Preprocessing(
            source=source, project=project, zone=zone, **preprocess_args)

        dlp_table = preprocess.get_dlp_table_per_block(
            50000, table_name, start_index)
        return table_name, dlp_table

    def inspect_table(table_dlp_table_tuple: Tuple) -> Tuple[str, Dict]:
        """Inspect table and retrieve finding results for each block.

        Args:
            table_dlp_table_list (Tuple): A tuple containing
            the table name and DLP table object.

        Returns:
            Tuple: A tuple containing the table name and finding results.
        """
        table_name, dlp_table = table_dlp_table_tuple
        dlpinspection = DlpInspection(project_id=project,
                                      location_category=location_category)

        finding_results_per_block = dlpinspection.get_finding_results(
            dlp_table)
        return table_name, finding_results_per_block

    def merge_and_top_finding(finding_tuple: Tuple) -> Tuple:
        """Merge and extract the top finding result for each table.

        Args:
            finding_tuple (Tuple): A tuple containing the table
            name and its corresponding finding_results.

        Returns:
            Tuple: A tuple containing the table name
            and the top finding result.
        """
        table_name, finding_results = finding_tuple

        dlpinspection = DlpInspection(project_id=project,
                                      location_category=location_category,)
        top_finding = dlpinspection.merge_finding_results(finding_results)
        return table_name, top_finding

    def process_catalog(top_finding_tuple: Tuple) -> None:
        """Process the top finding_result for a table and create a tag template
        for BigQuery tables and custom entries for Cloud SQL.

        Args:
            top_finding_tuple (Tuple): A tuple containing the table name
            and the top finding result.
        """
        table_name, top_finding = top_finding_tuple

        catalog = Catalog(
            data=top_finding,
            project_id=project,
            zone=zone,
            dataset=dataset,
            table=table_name,
            instance_id=instance_id,
            entry_group_name=entry_group_name
        )
        catalog.main()

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # pylint: disable=expression-not-assigned
        top_finding = (pipeline | 'Get_tables_info' >> beam.Create(get_tables_info())
                       | 'PreProcessTable' >> beam.Map(preprocess_table)
                       | 'Inspect' >> beam.Map(inspect_table)
                       | 'GroupByKey' >> beam.GroupByKey()
                       | 'ProcessTopFinding' >> beam.Map(merge_and_top_finding)
                       )
        top_finding | 'WriteOutput' >> beam.io.WriteToText(
            output_txt_location)
        top_finding | 'ProcessCatalog' >> beam.Map(process_catalog)


if __name__ == "__main__":
    parse_dataflow = parse_arguments()
    arguments = parse_dataflow.parse_args()
    run(arguments)
