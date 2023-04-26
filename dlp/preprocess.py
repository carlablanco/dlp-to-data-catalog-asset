# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Processes input data to fit to DLP inspection standards."""

from typing import List
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, dlp_v2
from google.cloud.sql.connector import Connector
from getpass import getpass


class Preprocessing:
    """Converts input data into Data Loss Prevention tables."""

    def __init__(self, db: str, project: str, dataset: str = None,
                 table: str = None, instance: str = None, zone: str = None,
                 db_user: str = None, database: str = None):
        """
        Args:
            project (str): The name of the Google Cloud Platform project.
            dataset (str): The name of the BigQuery dataset.
            table (str, optional): The name of the BigQuery table. Optional.
                Defaults to None.
        """

        self.project = project
        self.db = db

        if db == 'bigquery':
            self.bq_client = bigquery.Client(project=project)
            self.dataset = dataset
            self.table = table
        elif db == 'cloudsql-mysql' or db == 'cloudsql-postgres':
            self.connector = Connector()
            self.connection_name = f'{project}:{zone}:{instance}'
            self.database = database
            self.db_user = db_user
            self.db_password = getpass("Enter DB password: ")
            self.table = table

    def get_query_mysql(self, connection_name: str, db_user: str,
                        db_password: str, database: str, table: str):
        """_summary_

        Args:
            connection_name (str): _description_
            db_user (str): _description_
            db_password (str): _description_
            database (str): _description_
            table (str): _description_

        Returns:
            _type_: _description_
        """
        conn = self.connector.connect(
            connection_name,
            "pymysql",
            user=db_user,
            password=db_password,
            db=database
        )
        with conn.cursor() as cursor:
            query = f"select * from {table} limit 10"
            cursor.execute(query)
            rows = [row for row in cursor.fetchall()]
            return rows

    def get_query_postgres(self, connection_name: str, db_user: str,
                           db_password: str, database: str, table: str):
        """_summary_

        Args:
            connection_name (str): _description_
            db_user (str): _description_
            db_password (str): _description_
            database (str): _description_
            table (str): _description_

        Returns:
            _type_: _description_
        """
        conn = self.connector.connect(
            connection_name,
            "pg8000",
            user=db_user,
            password=db_password,
            db=database
        )
        print(conn)
        cursor = conn.cursor()
        query = f"select * from {table} limit 10"
        cursor.execute(query)
        rows = [row for row in cursor.fetchall()]
        return rows

    def get_query(self, table_id: str) -> str:
        """Creates an SQL query as string.

        Args:
            table_id (str): Fully qualified BigQuery tablename.

        Returns:
            str: SQL query as string.
        """
        query = f"SELECT *  FROM `{table_id}`"
        return query

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

    def get_bigquery_data(self, table_id: str) -> tuple:
        """Retrieves the schema and content of a BigQuery table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            tuple: A tuple containing the BigQuery schema and content.
        """
        try:
            table_bq = self.bq_client.get_table(table_id)
        except NotFound as exc:
            raise ValueError(f"Error retrieving table {table_id}.") from exc

        table_schema = table_bq.schema
        bq_schema = [schema_field.to_api_repr()
                     for schema_field in table_schema]

        sql_query = self.get_query(table_id)
        query_job = self.bq_client.query(sql_query)
        query_results = query_job.result()

        bq_rows_content = [dict(row) for row in query_results]

        return bq_schema, bq_rows_content

    def convert_to_dlp_table(self, bq_schema: List[dict],
                             bq_content: List[dict]) -> dlp_v2.Table:
        """Converts a BigQuery table into a DLP table.

        Converts a BigQuery table into a Data Loss Prevention table,
        an object that can be inspected by Data Loss Prevention.

        Args:
            bq_schema (list): The schema of a BigQuery table.
            bq_content (list): The content of a BigQuery table.

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

        if self.db == 'bigquery':
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
        elif self.db == 'cloudsql-mysql':
            print(self.get_query_mysql(self.connection_name, self.db_user,
                  self.db_password, self.database, self.table))
        elif self.db == 'cloudsql-postgres':
            print(self.connection_name)
            print(self.db_user)
            print(self.db_password)
            print(self.database)
            print(self.table)
            print(self.get_query_postgres(self.connection_name,
                  self.db_user, self.db_password, self.database, self.table))

        return dlp_tables_list
