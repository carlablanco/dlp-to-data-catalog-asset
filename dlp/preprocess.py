# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Processes input data to fit to DLP inspection standards."""

from typing import List, Tuple, Dict
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, dlp_v2

class Preprocessing:
    """Converts input data into Data Loss Prevention tables."""

    def __init__(self, project: str, dataset: str, table: str = None):
        """
        Args:
            project (str): The name of the Google Cloud Platform project.
            dataset (str): The name of the BigQuery dataset.
            table (str, optional): The name of the BigQuery table. Optional.
                Defaults to None.
        """
        self.bq_client = bigquery.Client(project=project)
        self.project = project
        self.dataset = dataset
        self.table = table

    def get_bigquery_tables(self, dataset: str) -> List[str]:
        """Constructs a list of table names from a BigQuery dataset.

        Args:
            Dataset (str): Name of the dataset in BigQuery.

        Returns:
            List of tablenames.
        """
        dataset_tables = list(self.bq_client.list_tables(dataset))
        table_names = [table.table_id for table in dataset_tables]
        return table_names

    def fetch_rows(self, table_id: str) -> List[Dict]:
        """Fetches a batch of rows from a BigQuery table.

           Args:
              table_id (str) = The path of the table were the data is fetched.

           Returns:
              List[Dict]: A list of rows, where each row is a tuple
              containing the values for each field in the table schema.
         """
        content = []
        fields = table_id.schema
        rows_iter = self.bq_client.list_rows(table_id)

        if not rows_iter.total_rows:
            print(f"""The Table {table_id} is empty. Please populate the
                                                        table and try again.""")
        else:
            for row in rows_iter:
                row_dict = {}
                for i, field in enumerate(fields):
                    row_dict[field.name] = row[i]
                content.append(row_dict)
        return content

    def get_bigquery_data(self, table_id: str) -> Tuple[List[Dict], List[Dict]]:
        """Retrieves the schema and content of a BigQuery table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            Tuple: A tuple containing the BigQuery schema and content as a List
            of Dictionaries.
        """
        try:
            table_bq = self.bq_client.get_table(table_id)
        except NotFound as exc:
            raise ValueError(f"Error retrieving table {table_id}.") from exc

        table_schema = table_bq.schema
        bq_schema = [schema_field.to_api_repr()
                     for schema_field in table_schema]

        bq_rows_content = self.fetch_rows(table_bq)

        return bq_schema, bq_rows_content

    def convert_to_dlp_table(self, bq_schema: List[Dict],
                             bq_content: List[Dict]) -> dlp_v2.Table:
        """Converts a BigQuery table into a DLP table.

        Converts a BigQuery table into a Data Loss Prevention table,
        an object that can be inspected by Data Loss Prevention.

        Args:
            bq_schema (List): The schema of a BigQuery table.
            bq_content (List): The content of a BigQuery table.

        Returns:
            A table object that can be inspected by Data Loss Prevention.
        """
        table_dlp = dlp_v2.Table()
        table_dlp.headers = [
            {"name": schema_object['name']} for schema_object in bq_schema
        ]

        rows = []
        for row in bq_content:
            rows.append(dlp_v2.Table.Row(
                values=[dlp_v2.Value(
                    string_value=str(cell_val)) for cell_val in row.values()]))

        table_dlp.rows = rows

        return table_dlp

    def get_dlp_table_list(self) -> List[dlp_v2.Table]:
        """Constructs a list of DLP Table objects

        Returns:
            A list of Data Loss Prevention table objects.
        """
        dlp_tables_list = []

        if self.table:
            bigquery_tables = [self.table]
        else:
            bigquery_tables = self.get_bigquery_tables(self.dataset)

        if bigquery_tables:
            for table_name in bigquery_tables:
                schema, content = self.get_bigquery_data(
                    f'{self.project}.{self.dataset}.{table_name}')
                table_dlp = self.convert_to_dlp_table(schema, content)
                dlp_tables_list.append(table_dlp)

        return dlp_tables_list
