# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a dataset and tags the results in Data Catalog."""

import argparse
from typing import Type
import os
import sys
from dotenv import load_dotenv
from dlp.preprocess import Preprocessing
from dlp.inspection import DlpInspection

load_dotenv()

def parse_arguments() -> Type[argparse.Namespace]:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser()
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
        required=True,
        help="The BigQuery dataset to be scanned.")
    parser.add_argument(
        "--table",
        type=str,
        help="""The BigQuery table to be scanned. Optional.
                If None, the entire dataset will be scanned.""")
    return parser.parse_args()

# pylint: disable=unused-argument


def run(mode:str, project: str, language_code: str, dataset: str= None,
        table: str = None, instance:str = None, zone:str = None, db:str = None,
        db_user:str = None, db_password:str = None):
    """Runs DLP inspection scan and tags the results to Data Catalog.

    Args:
        project: Project ID for which the client acts on behalf of.
        language_code: The BCP-47 language code to use, e.g. 'en-US'.
        dataset: The BigQuery dataset to be scanned.
        table: The BigQuery table to be scanned. Optional.
                If None, the entire dataset will be scanned.
    """
    preprocess = Preprocessing(
        mode=mode, project=project, dataset=dataset, table=table, instance=instance,
        zone= zone, db_user=db_user, db_password=db_password, db=db)
    tables = preprocess.get_dlp_table_list()
    print(tables)
    inspection = DlpInspection(project_id=project,
                               language_code=language_code, tables=tables)
    inspection.main()


if __name__ == "__main__":
    # args = parse_arguments()
    project = os.getenv('PROJECT')
    dataset = os.getenv('DATASET')
    language_code = os.getenv('LANGUAGE_CODE')
    table = os.getenv('TABLE')
    mode = os.getenv('MODE')
    instance = os.getenv('INSTANCE')
    zone = os.getenv('ZONE')
    db = os.getenv('DB')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    run(mode, project, language_code,
        dataset, table, instance, zone, db, db_user, db_password)
