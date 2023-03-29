# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Parses arguments before running DLP Scan."""
import argparse
from typing import List, Type
import sys

def parse_arguments(argv: List[str]) -> Type[argparse.Namespace]:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project",
        type=str,
        required=True,
        help="The project ID of the Cloud project to be used.")
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="The dataset ID of the BigQuery dataset to be used.")
    parser.add_argument(
        "--table",
        type=str,
        help="The table of the bigquery dataset to be used.")
    parser.add_argument(
        "--parent",
        type=str,
        required=True,
        help="The group that the infotype belongs to.")
    parser.add_argument(
        "--location",
        type=str,
        required=True,
        help="The default location to be used.")

    return parser.parse_args(argv[0:])    

def run(proyecto: str, dataset: str, table: str, parent: str, location: str):
    """Run the main function with the arguments parsed."""
    main(proyecto, dataset, table, parent, location)
    raise NotImplementedError

if __name__ == "__main__":
    arg = parse_arguments(sys.argv)
    run(arg.project, arg.dataset,  arg.table, arg.parent, arg.location)
