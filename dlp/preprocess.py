# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Processes input data to fit to DLP inspection standards."""

import dataclasses
from enum import Enum
from typing import List, Tuple, Dict

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
    service_account: str
    db_name: str
    table: str
    driver: str
    connection_type: str


class Database(Enum):
    """Represents available sources for database connections."""
    BIGQUERY = "bigquery"
    CLOUDSQL = "cloudsql"


class Preprocessing:
    """Converts input data into Data Loss Prevention tables."""

    def __init__(self, source: str, project: str,
                 bigquery_args: Dict = None, cloudsql_args: Dict = None):
        """Initializes `Preprocessing` class with arguments.

        Args:
            source (str): The name of the source of data used.
            project (str): The name of the Google Cloud Platform project.
            bigquery_args(Dict):
                dataset (str): The name of the BigQuery dataset.
                table (str, optional): The name of the BigQuery table. If not
                    provided, the entire dataset is scanned. Optional.
                    Defaults to None.
            cloudsql_args(Dict):
                instance (str): Name of the database instance.
                zone(str): The name of the zone.
                service_account(str): Service account email to be used.
                db_name(str): The name of the database.
                table (str): The name of the table.
                db_type(str): The type of the database. e.g. postgres, mysql.
        """
        self.source = Database(source)
        self.project = project
        if self.source == Database.BIGQUERY:
            self.bigquery = Bigquery(bigquery.Client(
                project=project),
                bigquery_args["dataset"],
                bigquery_args["table"])
        elif self.source == Database.CLOUDSQL:
            zone = cloudsql_args["zone"]
            instance = cloudsql_args["instance"]
            db_type = cloudsql_args["db_type"]
            if db_type == "mysql":
                driver = "pymysql"
                connection_name = f"mysql+{driver}"
            elif db_type == "postgres":
                driver = "pg8000"
                connection_name = f"postgresql+{driver}"

            self.cloudsql = CloudSQL(
                Connector(),
                f"{project}:{zone}:{instance}",
                cloudsql_args["service_account"],
                cloudsql_args["db_name"],
                cloudsql_args["table"],
                driver,
                connection_name)

    def get_connection(self):
        """Returns a connection to the database.

        Returns:
            A connection object that can be used to execute queries.
        """
        connector = self.cloudsql.connector.connect(
            self.cloudsql.connection_name,
            self.cloudsql.driver,
            enable_iam_auth=True,
            user=self.cloudsql.service_account,
            db=self.cloudsql.db_name
        )
        return connector

    def get_cloudsql_data(self, table: str) -> Tuple[List, List]:
        """Retrieves the schema and content of a table from CloudSQL.

        Args:
            table (str): The name of the table.

        Returns:
            Tuple(List, List): A tuple containing the schema and content
            as a List.
        """
       # Create a database engine instance.
        engine = create_engine(
            f'{self.cloudsql.connection_type}://', creator=self.get_connection)

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

        rows_iter = self.bigquery.bq_client.list_rows(table_id)

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

    def get_table_schema(self, table_id: str) -> Tuple[List, List, List]:
        """Generates a schema for a given table ID.

            Args:
                table_id (str): The ID of the table for which the schema needs
                to be generated.

            Returns:
                tuple: A tuple containing three lists - schema, nested_columns,
                and record_columns.
                - schema (list): The list of fields in the schema.
                - nested_columns (list): A list with the columns of the
                    nested columns.
                - record_columns (list): The columns with the record type.
            """
        schema = []
        nested_columns = []
        record_columns = []
        fields = table_id.schema
        for field in fields:
            record, nested, main_field = self.get_field(field)
            if nested:
                record_columns.append(main_field)
                nested_columns.append(record)
            else:
                schema.append(record)

        return schema, nested_columns, record_columns

    def get_field(self, field):
        """Generates a field for the given field object.

        Args:
            field: The field object for which the field needs to be generated.

        Returns:
            tuple: A tuple containing three values - record, nested, and
            main_cell.
            - record: The generated field or list of nested fields.
            - nested (bool): Indicates if the field is nested or not.
            - main_cell: The main field associated with the nested fields.
        """
        # Checks if the field has nested columns.
        if field.field_type == "RECORD":
            field_names = []
            for subfield in field.fields:
                main_cell = field.name
                cell = f"{field.name}.{subfield.name}"
                # Checks if the field has nested fields within.
                if subfield.field_type == "RECORD":
                    field_names.append(self.get_field(subfield))
                else:
                    field_names.append(cell)
            return field_names, True, main_cell
        return field.name, False, False

    def get_query(
            self, columns_selected: str, table_id: str, unnest: str) -> str:
        """Creates a SQL query as a string.

        Args:
           columns_selected (str): The string with the selected columns.
           table_id (str): The fully qualified name of the BigQuery table.
           unnest (str): The unnest string for the query.

        Returns:
            str: SQL query as a string.
        """
        query = f"""SELECT {columns_selected}
                    FROM `{table_id}`,
                    {unnest}"""
        return query

    def get_nested_types(self, table_id: str) -> List[str]:
        """ Gets the field modes of the selected table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            List: A complete list with the field modes of the columns.
        """
        nested_types = [field.mode for field in table_id.schema]
        return nested_types

    def get_rows_query(
        self,
        table_schema: List[Dict],
        nested_columns: List[Dict],
        record_columns: List[Dict],
        table_id: str) -> List[Dict]:
        """Retrieves the content of the table.

        Args:
            table_schema (List[Dict]): The schema of a BigQuery table.
            nested_columns (List[Dict]): A list with the columns
                of the nested columns.
            record_columns (List[Dict]): The columns with the record type.
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            List[Dict]: The content of the BigQuery table.
        """
        nested_types = self.get_nested_types(table_id)
        if "REPEATED" in nested_types:
            bq_schema = table_schema + record_columns
            columns_selected = ", ".join(str(column) for column in bq_schema)
            unnest = f"UNNEST ({record_columns[0]})"
        else:
            bq_schema = table_schema + nested_columns
            columns_selected = ", ".join(str(column) for column in bq_schema)
            unnest = (
                f"UNNEST ([{record_columns[0]}]) as {record_columns[0]} "
            )

        sql_query = self.get_query(columns_selected, table_id, unnest)

        query_job = self.bigquery.bq_client.query(sql_query)
        query_results = query_job.result()
        bq_rows_content = [dict(row) for row in query_results]

        return bq_rows_content

    def get_data_types(self, table_id: str) -> List:
        """ Gets the data types of the selected table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            List: A complete list with the data types of the columns.
        """
        return [field.field_type for field in table_id.schema]

    def flatten_list(self, unflattened_list: List) -> List:
        """
        Recursively flattens a nested list and returns a flattened list.

        Args:
            lista (list): The input list that needs to be flattened.

        Returns:
            list: The flattened list.
        """
        # Create an empty list to store the flattened elements.
        flattened = []

        # Iterate through each element in the list.
        for element in unflattened_list:
            # If the element is a list, recursively flatten the list.
            if isinstance(element, list):
                flattened.extend(self.flatten_list(element))
            else:
                # If the element is not a list, add it to the flattened list.
                flattened.append(element)

        # Return the flattened list.
        return flattened

    def get_bigquery_data(self, table_id: str) -> Tuple[List[Dict], List[Dict]]:
        """Retrieves the schema and content of a BigQuery table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            Tuple[List[Dict], List[Dict]]: A tuple containing the BigQuery
            schema and content as a List of Dictionaries.
        """
        try:
            table_bq = self.bigquery.bq_client.get_table(table_id)
        except NotFound as exc:
            raise ValueError(f"Error retrieving table {table_id}.") from exc

        dtypes = self.get_data_types(table_bq)

        # Checks if there are nested fields in the schema.
        if "RECORD" in dtypes:
            table_schema, nested_columns, record_columns = (
                self.get_table_schema(table_bq))
            table_schema = self.flatten_list(table_schema)
            nested_columns = self.flatten_list(nested_columns)
            bq_schema = table_schema + nested_columns
            bq_rows_content = self.get_rows_query(
                table_schema, nested_columns, record_columns, table_bq)
        else:
            table_schema = table_bq.schema
            bq_schema = [field.to_api_repr() for field in table_schema]
            bq_rows_content = self.fetch_rows(table_bq)

        return bq_schema, bq_rows_content

    def convert_to_dlp_table(self, schema: List[Dict],
                             content: List[Dict], table_id: str = None) ->(
                             dlp_v2.Table):
        """Converts a BigQuery table into a DLP table.

        Converts a BigQuery table into a Data Loss Prevention table,
        an object that can be inspected by Data Loss Prevention.

        Args:
            bq_schema (List[Dict]): The schema of a BigQuery table.
            bq_content (List[Dict]): The content of a BigQuery table.
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            A table object that can be inspected by Data Loss Prevention.
        """
        table_dlp = dlp_v2.Table()

        # Determine the column names based on the source
        if self.source == Database.BIGQUERY:
            table_bq = self.bigquery.bq_client.get_table(table_id)
            dtypes = self.get_data_types(table_bq)
            if "RECORD" in dtypes:
                table_dlp.headers = [{"name": name} for name in schema]
            else:
                table_dlp.headers = [{"name": item['name']} for item in schema]
        else:
            column_names = schema
            table_dlp.headers = [{"name": name} for name in column_names]

        rows = []
        for row in content:
            values = [dlp_v2.Value(string_value=str(cell_val))
                    for cell_val in row.values()] \
                    if self.source == Database.BIGQUERY \
                    else [dlp_v2.Value(string_value=str(cell_val))
                        for cell_val in row]

            rows.append(dlp_v2.Table.Row(values=values))

        table_dlp.rows = rows

        return table_dlp

    def get_dlp_table_list(self) -> List[dlp_v2.Table]:
        """Constructs a list of DLP Table objects

        Returns:
            A list of Data Loss Prevention table objects.
        """

        dlp_tables_list = []

        if self.source == Database.BIGQUERY:
            if self.bigquery.table:
                bigquery_tables = [self.bigquery.table]
            else:
                bigquery_tables = (
                    self.get_bigquery_tables(self.bigquery.dataset)
                )

            for table_name in bigquery_tables:
                table_id = (
                    f"{self.project}.{self.bigquery.dataset}.{table_name}")
                schema, content = self.get_bigquery_data(table_id)
                table_dlp = self.convert_to_dlp_table(
                    schema, content, table_id)
                dlp_tables_list.append(table_dlp)

        elif self.source == Database.CLOUDSQL:
            schema_content_list = [self.get_cloudsql_data(self.cloudsql.table)]
            dlp_tables_list = [
                self.convert_to_dlp_table(schema, content)
                for schema, content in schema_content_list
            ]

        return dlp_tables_list
