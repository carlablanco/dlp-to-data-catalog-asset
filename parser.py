# Argument pareser.
#
# This is a helper function to parse command line arguments.
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
        help="""The project ID of the Cloud project you want to use.""")
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="""The dataset ID of the BigQuery dataset you want to use.""")
    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="""The table ID of the BigQuery table you want to use.""")
    parser.add_argument(
        "--parent",
        type=str,
        required=True,
        help="""The parent of the infotype you want to inspect in the DLP API.""")
    parser.add_argument(
        "--location",
        type=str,
        required=True,
        help="""The location of the DataCatalog.""")
    return parser.parse_args(argv[0:])    

def run(proyecto: str, dataset: str, table: str, parent: str, location: str):
    # run the main function with the arguments parsed
    main(proyecto, dataset, table, parent, location) 

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv)
    run(arguments.proyecto, arguments.dataset, arguments.table, arguments.parent, arguments.location)