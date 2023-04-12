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
def run(project: str, language_code: str, dataset: str, table: str = None):
    """Runs DLP inspection scan and tags the results to Data Catalog.
    Args:
        project: Project ID for which the client acts on behalf of.
        language_code: The BCP-47 language code to use, e.g. 'en-US'.
        dataset: The BigQuery dataset to be scanned.
        table: The BigQuery table to be scanned. Optional.
                If None, the entire dataset will be scanned.
    """

    preprocess = Preprocessing(
        project=project, dataset=dataset, table=table)
    preprocess.get_dlp_table_list()


if __name__ == "__main__":
    args = parse_arguments()
    run(args.project, args.language_code,
        args.dataset, args.table)
