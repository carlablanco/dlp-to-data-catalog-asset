# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a BigQuery dataset and tags the results in Data Catalog."""

from google.cloud import bigquery
import json
import io
from typing import Optional


class BigQuerySchemaRows:
    """A class used to retrieve BigQuery schema rows and convert them to DLP table items.
    """
    def __init__(self, project: str,
                 dataset: str, table: Optional[str] = None):
        self.bq_client = bigquery.Client()
        self.project = project
        self.dataset = dataset
        self.table = table

    def get_query(self, table_id: str) -> str:
        """Makes the sql string to query.

        Args:
            table_id (str): The fully qualified name of the BQ table.

        Returns:
            str: Sql query
        """
        query = f"SELECT *  FROM `{table_id}`"
        return query

    def convert_to_dlp_table(self, table_id: str) -> dict:
        """Converts a BQ table into a table item that can be ingested by DLP.

        Args:
            table_id (str): The fully qualified name of the BQ table.

        Returns:
            dict: A table item that can be ingested by DLP.
        """
        # Obtención de esquema json de la tabla BQ
        table_bq = self.bq_client.get_table(table_id)
        f_var = io.StringIO("")
        self.bq_client.schema_to_json(table_bq.schema, f_var)
        bq_schema_json = json.loads(f_var.getvalue())

        # Mapeo de datos del header
        headers = []
        for i in bq_schema_json:
            headers.append({"name": i['name']})

        # Mapeo de los rows
        rows = []
        sql_query = self.get_query(table_id)
        query_job = self.bq_client.query(sql_query)
        bqrows = query_job.result()
        records = [dict(row) for row in bqrows]
        for row in records:
            rows.append(
                {"values": [{"string_value": str(cell_val)} for cell_val in row.values()]})

        table_dlp = {}
        table_dlp["headers"] = headers
        table_dlp["rows"] = rows
        item = {"table": table_dlp}
        return item

    def get_dlp_table_list(self) -> list:
        """Constructs a list of table items that can be ingested by DLP from a BigQuery dataset or table.

        Returns:
            list: A list of table items that can be ingested by DLP.
        """
        items = []
        if self.table:
            table_id = f'{self.project}.{self.dataset}.{self.table}'
            item = self.convert_to_dlp_table(table_id)
            items.append(item)
        else:
            tables = list(self.bq_client.list_tables(self.dataset))
            if tables:
                for table in tables:
                    table_id = f'{self.project}.{self.dataset}.{table.table_id}'
                    item = self.convert_to_dlp_table(table_id)
                    items.append(item)

        return items