# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Processes input data to fit to DLP inspection standards."""

from typing import List
import concurrent.futures
import math
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

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
        self.table_id = f'{project}.{dataset}.{table}'

    def fetch_rows(self, start_index):
        """Fetches a batch of rows from a BigQuery table.

        Args:
            start_index (int): The starting index of the batch.

        Returns:
            content (list): A list of rows, where each row is a tuple containing
            the values for each field in the table schema.
        """
        table = self.bq_client.get_table(self.table_id)
        fields = table.schema
        rows_iter = self.bq_client.list_rows(
            self.table_id,
            start_index=start_index,
            max_results=500
        )
        content = []
        [content.append(row[0:(len(fields))]) for row in rows_iter]
        return content

    def parallel_read(self):
        """Constructs a list with the content of the table

        Returns:
            rows (List[tuples]): Conetent of the table
        """
        table = self.bq_client.get_table(self.table_id)
        rows = []

        # Determine the number of rows and an appropriate level of parallelism
        num_rows = table.num_rows
        num_parallel = num_parallel = min(math.ceil(num_rows / 10000), 10)

        # Fetch rows in parallel threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=
                                                   num_parallel) as executor:
            futures = [executor.submit(self.fetch_rows, start_index)
                       for start_index in range(0, num_rows, 500)]
            for future in concurrent.futures.as_completed(futures):
                rows.extend(future.result())
        return rows

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

    def get_bigquery_data(self) -> tuple:
        """Retrieves the schema and content of a BigQuery table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            tuple: A tuple containing the BigQuery schema and content.
            
        """
        try:
            table_bq = self.bq_client.get_table(self.table_id)
        except NotFound as ex:
            raise ValueError(f"Error retrieving table {self.table_id}.") from ex

        table_schema = table_bq.schema
        bq_schema = [schema_field.to_api_repr()
                     for schema_field in table_schema]

        rows = self.parallel_read()

        return bq_schema, rows

    def convert_to_dlp_table(self, bq_schema: List[dict],
                             bq_content: List[tuple]) -> dict:
        """Converts a BigQuery table into a DLP table.

        Converts a BigQuery table into a Data Loss Prevention table,
        an object that can be inspected by Data Loss Prevention.

        Args:
            bq_schema (list): The schema of a BigQuery table.
            bq_content (list): The content of a BigQuery table.

        Returns:
            A table object that can be inspected by Data Loss Prevention.
        """
        headers = [{"name": i['name']} for i in bq_schema]

        rows = []
        for row in bq_content:
            rows.append(
                {"values":
                    [{"string_value":
                        str(cell_val)} for cell_val in row]}
            )

        table_dlp = {"table": {"headers": headers, "rows": rows}}
        return table_dlp

    def get_dlp_table_list(self) -> List[dict]:
        """Constructs a list of table objects.

        Constructs a list from the table objects that to be inspected
            by Data Loss Prevention.

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
                schema, content = self.get_bigquery_data(self.table_id)
                table_dlp = self.convert_to_dlp_table(schema, content)
                dlp_tables_list.append(table_dlp)

        return dlp_tables_list
