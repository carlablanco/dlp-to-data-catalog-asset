# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Parses arguments before running DLP Scan."""

import argparse
from typing import Type
from preprocess import Preprocessing


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
        "--location",
        type=str,
        required=True,
        help=" Location where the jobs will be run.")
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


def run(project: str, language_code: str, location: str, dataset: str, table: str = None):
    """Runs DLP inspection scan and tags the results to Data Catalog.
    Args:
        project: Project ID for which the client acts on behalf of.
        language_code: The BCP-47 language code to use, e.g. 'en-US'.
        location: Location where the jobs will be run.
        dataset: The BigQuery dataset to be scanned.
        table: The BigQuery table to be scanned. Optional.
                If None, the entire dataset will be scanned.
    """
    print(f"Project: {project}")
    print(f"Language Code: {language_code}")
    print(f"Location: {location}")
    print(f"Dataset: {dataset}")
    print(f"Table: {table}")
    preprocess = Preprocessing(
        project=project, dataset=dataset)
    items = preprocess.get_dlp_table_list()
    print(items)


if __name__ == "__main__":
    args = parse_arguments()
    run(args.project, args.language_code,
        args.location, args.dataset, args.table)
