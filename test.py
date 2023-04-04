# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs DLP inspection on a BigQuery dataset and tags the results in Data Catalog."""

from google.cloud import bigquery


class Preprocessing:
    """Class for preprocessing tables into Data Loss Prevention tables.
    """

    def __init__(self, project: str, dataset: str, table: str = None):
        """
        Args:
            project (str): The name of the GCP project.
            dataset (str): The name of the BigQuery dataset.
            table (str, optional): The name of the BigQuery table.
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
        table_schema = table_bq.schema
        bq_schema = [schema_field.to_api_repr()
                     for schema_field in table_schema]

        sql_query = self.get_query(table_id)
        query_job = self.bq_client.query(sql_query)
        query_results = query_job.result()

        bq_rows_content = [dict(row) for row in query_results]

        return bq_schema, bq_rows_content

    def convert_to_dlp_table(self, bq_schema: list,
                             bq_rows_content: list) -> dict:
        """Converts a BigQuery table into an object that can be inspected
        by Data Loss Prevention.
        Args:
            bq_schema (list): The schema of a BigQuery table.
            bq_rows_content (list): The content of a BigQuery table.
        Returns:
            dict: A table object that can be inspected by Data Loss Prevention. le sumamos algunoa caracteres  ver si tira un error 
        """

        headers = [{"name": i['name']} for i in bq_schema]

        rows = []
        for row in bq_rows_content:
            rows.append(
                {"values":
                    [{"string_value":
                        str(cell_val)} for cell_val in row.values()]}
            )

        table_dlp = {"table": {"headers": headers, "rows": rows}}

        return table_dlp

    def get_dlp_table_list(self) -> list:
        """Constructs a list of table objects that can be inspected
        by Data Loss Prevention.
        Returns:
            list: A list of table object that can be inspected
            by Data Loss Prevention.
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