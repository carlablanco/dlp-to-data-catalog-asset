# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Processes input data to fit to DLP inspection standards."""

from typing import List, Tuple
import subprocess
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, dlp_v2
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, MetaData, Table


class Preprocessing:
    """Converts input data into Data Loss Prevention tables."""

    def __init__(self, db_source: str, project: str, dataset: str = None,
                 table: str = None, instance: str = None, zone: str = None,
                 database: str = None):
        """
        Args:
            project (str): The name of the Google Cloud Platform project.
            dataset (str): The name of the BigQuery dataset.
            table (str, optional): The name of the BigQuery table. Optional.
                Defaults to None.
            instance (str, optional): Name of the database instance. Optional.
            zone(str, optional): The name of the zone. Optional.
            database(str, optional): The name of the database. Optional.
        """
        self.project = project
        self.db_source = db_source

        if db_source == 'bigquery':
            self.bq_client = bigquery.Client(project=project)
            self.dataset = dataset
            self.table = table
        elif db_source in ['cloudsql-mysql', 'cloudsql-postgres']:
            self.connector = Connector()
            self.connection_name = f'{project}:{zone}:{instance}'
            self.database = database
            self.table = table

    def get_connection(self):
        """Return a connection to the database.

        Returns:
        A connection object that can be used to execute queries on the database.
        """
        if self.db_source == 'cloudsql-mysql':
            driver = "pymysql"
        if self.db_source == 'cloudsql-postgres':
            driver = "pg8000"

        user_result = subprocess.run(
            ['gcloud', 'config', 'get-value', 'account'],
            capture_output=True, text=True, check=True)
        gcloud_user = user_result.stdout.strip()
        conn = self.connector.connect(
            self.connection_name,
            driver,
            user=gcloud_user,
            enable_iam_auth=True,
            db=self.database
        )
        return conn

    def get_cloudsql_data(self, table: str) -> Tuple[List]:
        """Retrieve the schema and content of a table from a Cloud SQL database.

        Args:
            table (str): The name of the table.

        Returns:
            Tuple[List]: A tuple containing the schema and content as a List.
        """
        if self.db_source == 'cloudsql-mysql':
            connection_type = "mysql+pymysql"
        if self.db_source == 'cloudsql-postgres':
            connection_type = "postgresql+pg8000"

       # Create a database engine instance
        engine = create_engine(f'{connection_type}://',
                               creator=self.get_connection)

        # Create a Metadata and Table instance
        metadata = MetaData()
        table = Table(table, metadata, extend_existing=True,
                      autoload_with=engine)

        # Get table schema
        schema = [column.name for column in table.columns]

        # Get table contents
        with engine.connect() as connection:
            select = table.select().with_only_columns(table.columns)
            content = list(connection.execute(select).fetchall())

        return schema, content

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

    def fetch_rows(self, table_id: str) -> List[Tuple]:
        """Fetches a batch of rows from a BigQuery table.

           Args:
              table_id (str) = The path of the table were the data is fetched.

           Returns:
              List[Tuple]: A list of rows, where each row is a tuple
              containing the values for each field in the table schema.
         """
        content = []
        rows_iter = self.bq_client.list_rows(table_id)

        if not rows_iter.total_rows:
            print(f"""The Table {table_id} is empty. Please populate the
                                                        table and try again.""")
        else:
            for row in rows_iter:
                content.append(tuple(row))
        return content

    def get_bigquery_data(self, table_id: str) -> Tuple[List[str], List[Tuple]]:
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
        bq_schema = [schema_field.to_api_repr()['name']
                     for schema_field in table_schema]
        bq_rows_content = self.fetch_rows(table_bq)
        return bq_schema, bq_rows_content

    def convert_to_dlp_table(self, schema: List[List],
                             content: List[List]) -> dlp_v2.Table:
        """Converts a BigQuery table into a DLP table.

        Converts a BigQuery table into a Data Loss Prevention table,
        an object that can be inspected by Data Loss Prevention.

        Args:
            bq_schema (List[Dict]): The schema of a BigQuery table.
            bq_content (List[Dict]): The content of a BigQuery table.

        Returns:
            A table object that can be inspected by Data Loss Prevention.
        """
        table_dlp = dlp_v2.Table()
        table_dlp.headers = [
            {"name": schema_object} for schema_object in schema
        ]

        rows = []
        for row in content:
            rows.append(dlp_v2.Table.Row(
                values=[dlp_v2.Value(
                    string_value=str(cell_val)) for cell_val in row]))

        table_dlp.rows = rows

        return table_dlp

    def get_dlp_table_list(self) -> List[dlp_v2.Table]:
        """Constructs a list of DLP Table objects

        Returns:
            A list of Data Loss Prevention table objects.
        """
        dlp_tables_list = []

        if self.db_source == 'bigquery':
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
        elif self.db_source in ['cloudsql-mysql', 'cloudsql-postgres']:
            schema, content = self.get_cloudsql_data(self.table)
            table_dlp = self.convert_to_dlp_table(schema, content)
            dlp_tables_list.append(table_dlp)

        return dlp_tables_list
