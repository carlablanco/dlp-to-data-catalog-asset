# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Processes input data to fit to DLP inspection standards."""

from typing import List, Tuple, Dict
import subprocess
import dataclasses
from enum import Enum
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, dlp_v2
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, MetaData, Table


@dataclasses.dataclass
class Bigquery:
    """Represents a connection to a Google BigQuery dataset and table."""
    bq_client: bigquery.Client
    dataset: str
    table: str


@dataclasses.dataclass
class CloudSQL:
    """Represents a connection to a Google CloudSQL."""
    connector: Connector
    connection_name: str
    database: str
    table: str


class Source(Enum):
    """Represents available sources for database connections."""
    BIGQUERY: str = "bigquery"
    POSTGRES: str = "cloudsql-postgres"
    MYSQL: str = "cloudsql-mysql"


class Preprocessing:
    """Converts input data into Data Loss Prevention tables."""

    def __init__(self, source: Source, project: str,
                 bigquery_args: Dict = None, cloudsql_args: Dict = None):
        """Initializes `Preprocessing` class with arguments.

        Args:
            source (str): The name of the source of data used.
            project (str): The name of the Google Cloud Platform project.
            bigquery_args(Dict): 
                dataset (str): The name of the BigQuery dataset.
                table (str, optional): The name of the BigQuery table. Optional.
                    Defaults to None.
            cloudsql_args(Dict):
                instance (str, optional): Name of the database instance.
                zone(str, optional): The name of the zone. Optional.
                database(str, optional): The name of the database.
                table (str, optional): The name of the table.
        """
        self.source = source

        if self.source == Source.BIGQUERY:
            self.bigquery = Bigquery(bigquery.Client(
                project=project), bigquery_args["dataset"],
                bigquery_args["table"])
        elif self.source in [Source.MYSQL, Source.POSTGRES]:
            zone = cloudsql_args["zone"]
            instance = cloudsql_args["instance"]
            self.cloudsql = CloudSQL(
                Connector(),
                f"{project}:{zone}:{instance}",
                cloudsql_args["database"], cloudsql_args["table"])

    def get_connection(self):
        """ReturnS a connection to the database.

        Returns:
        A connection object that can be used to execute queries on the database.
        """
        if self.source == Source.MYSQL:
            driver = "pymysql"
        if self.source == Source.POSTGRES:
            driver = "pg8000"

        user_result = subprocess.run(
            ["gcloud", "config", "get-value", "account"],
            capture_output=True, text=True, check=True)
        gcloud_user = user_result.stdout.strip()
        conn = self.cloudsql.connector.connect(
            self.cloudsql.connection_name,
            driver,
            user=gcloud_user,
            enable_iam_auth=True,
            db=self.cloudsql.database
        )
        return conn

    def get_cloudsql_data(self, table: str) -> Tuple[List]:
        """Retrieves the schema and content of a table from CloudSQL.

        Args:
            table (str): The name of the table.

        Returns:
            Tuple[List]: A tuple containing the schema and content as a List.
        """
        if self.source == Source.MYSQL:
            connection_type = "mysql+pymysql"
        if self.source == Source.POSTGRES:
            connection_type = "postgresql+pg8000"

       # Create a database engine instance.
        engine = create_engine(f'{connection_type}://',
                               creator=self.get_connection)

        # Create a Metadata and Table instance.
        metadata = MetaData()
        table = Table(table, metadata, extend_existing=True,
                      autoload_with=engine)

        # Get table schema.
        schema = [column.name for column in table.columns]

        # Get table contents.
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
        dataset_tables = list(self.bigquery.bq_client.list_tables(dataset))
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
        rows_iter = self.bigquery.bq_client.list_rows(table_id)

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
            table_bq = self.bigquery.bq_client.get_table(table_id)
        except NotFound as exc:
            raise ValueError(f"Error retrieving table {table_id}.") from exc

        table_schema = table_bq.schema
        bq_schema = [schema_field.to_api_repr()["name"]
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
        if self.source == Source.BIGQUERY:

            bigquery_tables = [self.bigquery.table] if self.bigquery.table \
                else self.get_bigquery_tables(self.bigquery.dataset)

            schema_content_list = [self.get_bigquery_data(
                f"{self.bigquery.dataset}.{table_name}")
                for table_name in bigquery_tables]

        elif self.source in [Source.MYSQL, Source.POSTGRES]:
            schema_content_list = [self.get_cloudsql_data(self.cloudsql.table)]

        return [self.convert_to_dlp_table(
            schema, content) for schema, content in schema_content_list]
