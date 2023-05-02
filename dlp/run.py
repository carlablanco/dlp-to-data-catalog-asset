# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a dataset and tags the results in Data Catalog."""

import argparse
from typing import Type
from dlp.preprocess import Preprocessing
from dlp.inspection import DlpInspection
from getpass import getpass

def parse_arguments() -> Type[argparse.Namespace]:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--DB",
        type=str,
        required=True,
        help="The source of data used, e.g. 'cloudsql-postgres' 'cloudsql-mysql' 'bigquery'.")
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

# pylint: disable=unused-argument
def run(db_source:str, project: str, language_code: str, dataset: str= None,
        table: str = None, instance:str = None, zone:str = None,
        database:str = None):
    """Runs DLP inspection scan and tags the results to Data Catalog.

    Args:
        project: Project ID for which the client acts on behalf of.
        language_code: The BCP-47 language code to use, e.g. 'en-US'.
        dataset: The BigQuery dataset to be scanned.
        table: The BigQuery table to be scanned. Optional.
                If None, the entire dataset will be scanned.
    """ 
    preprocess = Preprocessing(
        db_source=db_source, project=project, dataset=dataset, table=table,
        instance=instance, zone= zone, database=database)
    tables = preprocess.get_dlp_table_list()
    print(tables)
    inspection = DlpInspection(project_id=project,
                               language_code=language_code, tables=tables)
    inspection.main()

if __name__ == "__main__":
    args = parse_arguments()
    run(args.DB, args.project, args.language_code,
        args.dataset, args.table, args.instance, args.zone,
        args.database)
