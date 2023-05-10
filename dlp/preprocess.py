# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Processes input data to fit to DLP inspection standards."""

import pandas as pd
import itertools
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

    def get_table_schema(self, table_id):
        """Generates a schema for a given table ID.

            Args:
                table_id (str): The ID of the table for which the schema needs
                to be generated.

            Returns:
                tuple: A tuple containing three lists - schema, nested_schema, and
                main_nested_schema.
                    - schema (list): The list of fields in the schema.
                    - nested_schema (list): The list of nested fields in the schema.
                    - main_nested_schema (list): The list of main fields associated
                    with the nested fields.
            """
        schema = []
        nested_schema = []
        main_nested_schema = []
        fields = table_id.schema
        for field in fields:

            record, nested, main_field = self.get_field(field)
            if nested == True:
                main_nested_schema.append(main_field)
                nested_schema.append(record)
            else:
                schema.append(record)

        return schema, nested_schema, main_nested_schema

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
        nested = False
        main_cell = False
        if field.field_type == "RECORD":
            recordField = []
            for subfield in field.fields:
                main_cell = field.name
                cell = field.name+"."+subfield.name
                if subfield.field_type == "RECORD":
                    recordField.append(self.get_field(subfield))
                else:
                    recordField.append(cell)
            nested = True
            return recordField, nested, main_cell
        else:
            return field.name, nested, main_cell

    def get_query(self, bq_schema, main_nested_schema, table_id):
        """Creates an SQL query as string.

        Args:
           bq_schema (List): The schema of a BigQuery table.
           main_nested_schema(List) : The columns with record type.
           table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            str: SQL query as string.
        """

        columns_selected = ', '.join(str(column) for column in bq_schema)

        unnest = f"""UNNEST ([{main_nested_schema[0]}]) as 
                    {main_nested_schema[0]}"""
        query = f"""SELECT {columns_selected} 
        FROM `{table_id}`, 
        {unnest}""" 
        return query

    def get_query_array(self, bq_schema, main_nested_schema, table_id):
        """Creates an SQL query as string.

        Args:
           bq_schema (List): The schema of a BigQuery table.
           main_nested_schema(List) : The columns with record type.
           table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            str: SQL query as string. 
        """

        columns_selected = ', '.join(str(column) for column in bq_schema)

        unnest = f"""UNNEST ({main_nested_schema[0]})"""
        query = f"""SELECT {columns_selected} 
            FROM `{table_id}`, 
            {unnest}""" 
        print(query)
        return query

    def get_rows_query(self, table_schema, nested_schema, main_nested_schema, table_id):
        """ Retrives the content of the table.

        Args:
            bq_schema (List): The schema of a BigQuery table.
            main_nested_schema(List) : The columns with record type.
            table_id (str): The fully qualified name of the BigQuery table.

       Returns:
            List[Dict]: The content of the BigQuery table.
        """
        nested_types = self.get_nested_types(table_id)
        if "REPEATED" in nested_types:
            bq_schema = table_schema + main_nested_schema
            sql_query = self.get_query_array(bq_schema,main_nested_schema, table_id)
            query_job = self.bq_client.query(sql_query)
            query_results = query_job.result()

        else:
            bq_schema = table_schema + nested_schema
            sql_query = self.get_query(bq_schema,main_nested_schema, table_id)
            query_job = self.bq_client.query(sql_query)
            query_results = query_job.result()

        bq_rows_content = [dict(row) for row in query_results]

        return bq_rows_content

    def get_nested_types(self, table_id):
        """ Gets the field modes of the selected table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            List: A complete list with the field modes of the columns.
        """
        nested_types = []

        fields = table_id.schema
        for field in fields:
            nested_types.append(field.mode)

        return nested_types

    def get_data_types(self, table_id):
        """ Gets the data types of the selected table.

        Args:
            table_id (str): The fully qualified name of the BigQuery table.

        Returns:
            List: A complete list with the data types of the columns.
        """
        dtypes = []

        fields = table_id.schema
        for field in fields:
            dtypes.append(field.field_type)

        return dtypes

    def get_bigquery_data(self, table_id: str) -> Tuple[List[Dict],
                                                        List[Dict]]:
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

        dtypes = self.get_data_types(table_bq)

        if "RECORD" in dtypes:
            table_schema, nested_schema, main_nested_schema = (
                self.get_table_schema(table_bq))
            table_schema = self.flatten_list(table_schema)
            nested_schema = self.flatten_list(nested_schema)
            bq_schema = table_schema + nested_schema

            bq_rows_content = self.get_rows_query(table_schema, nested_schema,
                                                  main_nested_schema, table_bq)
        else:
            table_schema = table_bq.schema
            bq_schema = [schema_field.to_api_repr()
                         for schema_field in table_schema]
            bq_rows_content = self.fetch_rows(table_bq)

        return bq_schema, bq_rows_content

    def flatten_list(self, lst):
        """
        Recursively flattens a nested list and returns a flattened list.

        Args:
            lst (list): The input list that needs to be flattened.

        Returns:
            list: The flattened list.
        """
        # Create an empty list to store the flattened elements.
        flattened = []

        # Iterate through each element in the list.
        for element in lst:
            # If the element is a list, recursively flatten the list.
            if isinstance(element, list):
                flattened += self.flatten_list(element)
            else:
                # If the element is not a list, add it to the flattened list.
                flattened.append(element)

        # Return the flattened list.
        return flattened

    def convert_to_dlp_table(self, bq_schema: List[Dict],
                             bq_content: List[Dict], table_id: str) ->(
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
        table_bq = self.bq_client.get_table(table_id)
        dtypes = self.get_data_types(table_bq)

        table_dlp = dlp_v2.Table()

        if "RECORD" in dtypes:

            table_dlp.headers = [
                {"name": name} for name in bq_schema
            ]
            rows = []
            for row in bq_content:
                rows.append(dlp_v2.Table.Row(
                    values=[dlp_v2.Value(
                        string_value=str(cell_val)) for cell_val
                        in row.values()]))
            table_dlp.rows = rows

        else:
            table_dlp.headers = [
                {"name": schema_object['name']} for schema_object in bq_schema
            ]

            rows = []
            for row in bq_content:
                rows.append(dlp_v2.Table.Row(
                    values=[dlp_v2.Value(
                        string_value=str(cell_val)) for cell_val in
                        row.values()]))

            table_dlp.rows = rows

        return table_dlp

    def get_dlp_table_list(self) -> List[dlp_v2.Table]:
        """Constructs a list of DLP Table objects.

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
                table_id = f'{self.project}.{self.dataset}.{table_name}'
                schema, content = self.get_bigquery_data(
                    table_id)

                table_dlp = self.convert_to_dlp_table(schema, content,
                                                      table_id)
                dlp_tables_list.append(table_dlp)

        return dlp_tables_list
