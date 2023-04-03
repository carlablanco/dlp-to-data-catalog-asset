# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a BigQuery dataset and tags the results in Data Catalog."""

import json
from typing import Optional
import io
from google.cloud import bigquery


class PreprocessBqToDlp:
    """Class for preprocessing BigQuery tables into Data Loss Prevention tables.
    """

    def __init__(self, project: str,
                 dataset: str, table: Optional[str] = None):
        """
        Args:
            project (str): The name of the GCP project where the BigQuery dataset exists.
            dataset (str): The name of the BigQuery dataset.
            table (str, optional): The name of the BigQuery table. Defaults to None.
        """
        self.bq_client = bigquery.Client()
        self.project = project
        self.dataset = dataset
        self.table = table

    def get_query(self, table_id: str) -> str:
        """Creates an SQL query as string.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            str: SQL query as string.
        """
        query = f"SELECT *  FROM `{table_id}`"
        return query

    def get_bigquery_table(self, table_id: str) -> tuple:
        """Retrieves the schema and content of a BigQuery table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            tuple: A tuple containing the BigQuery schema and content.
        """
        table_bq = self.bq_client.get_table(table_id)
        f_temp = io.StringIO("")
        self.bq_client.schema_to_json(table_bq.schema, f_temp)
        bq_schema = json.loads(f_temp.getvalue())

        sql_query = self.get_query(table_id)
        query_job = self.bq_client.query(sql_query)
        query_results = query_job.result()

        bq_rows_content = [dict(row) for row in query_results]

        return bq_schema, bq_rows_content

    def convert_to_dlp_table(self, bq_schema: dict, bq_rows_content: dict) -> dict:
        """Converts a BigQuery table into an object that can be inspected by Data Loss Prevention.

        Args:
            bq_schema (dict): The schema of a BigQuery table.
            bq_rows_content (dict): The content of a BigQuery table.

        Returns:
            dict: A table object that can be inspected by Data Loss Prevention.
        """

        headers = []
        for i in bq_schema:
            headers.append({"name": i['name']})

        rows = []
        for row in bq_rows_content:
            rows.append(
                {"values": [{"string_value": str(cell_val)} for cell_val in row.values()]})

        table_dlp = {"table": {"headers": headers, "rows": rows}}

        return table_dlp

    def get_dlp_table_list(self) -> list:
        """Constructs a list of table objects that can be inspected by Data Loss Prevention.

        Returns:
            list: A list of table object that can be inspected by Data Loss Prevention.
             
            table_dlp_list = [
                {
                "table": {
                    "headers": [
                        {
                            "name": "id"
                        },
                        {
                            "name": "name"
                        },
                        {
                            "name": "age"
                        }
                    ],
                    "rows": [
                        {
                            "values": [
                                {
                                    "string_value": "1"
                                },
                                {
                                    "string_value": "John"
                                },
                                {
                                    "string_value": "25"
                                }
                            ]
                        },
                        {
                            "values": [
                                {
                                    "string_value": "2"
                                },
                                {
                                    "string_value": "Mary"
                                },
                                {
                                    "string_value": "30"
                                }
                            ]
                        },
                    ]
                }
            }
            ]
            
        """

        table_dlp_list = []
        if self.table:
            table_id = f'{self.project}.{self.dataset}.{self.table}'
            bq_schema, bq_rows_content = self.get_bigquery_table(table_id)
            table_dlp = self.convert_to_dlp_table(bq_schema, bq_rows_content)
            table_dlp_list.append(table_dlp)
        else:
            tables = list(self.bq_client.list_tables(self.dataset))
            if tables:
                for table in tables:
                    table_id = f'{self.project}.{self.dataset}.{table.table_id}'
                    bq_schema, bq_rows_content = self.get_bigquery_table(
                        table_id)
                    table_dlp = self.convert_to_dlp_table(
                        bq_schema, bq_rows_content)
                    table_dlp_list.append(table_dlp)

        return table_dlp_list
