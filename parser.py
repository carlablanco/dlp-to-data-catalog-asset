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
        help="The Cloud project to be used.")
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="The BigQuery dataset to be used.")
    parser.add_argument(
        "--table",
        type=str,
        help="The BigQuery table to be used.")
    parser.add_argument(
        "--parent_infotype",
        type=str,
        required=True,
        help="The group that the Infotype belongs to.")
    parser.add_argument(
        "--location",
        type=str,
        required=True,
        help="The default location to be used.")

    return parser.parse_args(argv[0:])    


def run(project: str, dataset: str, table: str, parent_infotype: str, location: str):
    """Run the main function with the arguments parsed.
    Args:
        project: The Google Cloud project id to use. 
        dataset: The BigQuery dataset to be used.
        table: The BigQuery table to be used.
        parent_infotype:The group that the Infotype belongs to.
        location: The default location to be used.
    """
    raise NotImplementedError


if __name__ == "__main__":
    args = parse_arguments(sys.argv)
    run(args.project, args.dataset,  args.table, args.parent_infotype, args.location)
