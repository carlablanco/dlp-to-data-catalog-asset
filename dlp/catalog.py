# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Creates and attaches a tag template to a BigQuery table."""

from typing import List, Dict, Optional
import datetime
from google.cloud import datacatalog_v1

class Catalog:
    def __init__(self, data: List[Dict], project_id: str, location: str,
                 dataset: Optional[str] = None, table: Optional[str] = None,
                 instance_id: Optional[str] = None):
        """Initializes the class with the required data.

        Args:
            data(str): The data Previously inspected by the DLP API.
            project(str): Project ID for which the client acts on behalf of.
            location(str): The compute engine region.
            dataset(str): The BigQuery dataset to be scanned if it's BigQuery.
                        Optional. Default value is None.
            table(str): The name of the table if it's BigQuery.
                        Optional. Default value is None.
            instance(str): Name of the database instance if it's CloudSQL.
                           Optional. Default value is None.
        """
        self.client = datacatalog_v1.DataCatalogClient()
        self.tag_template = datacatalog_v1.TagTemplate()
        self.data = data
        self.project_id = project_id
        self.location = location
        self.dataset = dataset
        self.table = table
        self.instance_id = instance_id

        self.timestamp = int(datetime.datetime.now().timestamp())
        if self.instance_id is not None:
            self.entry_group_id = f"dlp_{self.instance_id}_{self.timestamp}"
            self.entry_id = f"dlp_{self.timestamp}"
        else:
            self.tag_template_id = f"""dlp_{self.dataset.lower()}_
            {self.table.lower()}_{self.timestamp}"""

    def create_tag_template(self, parent: str) -> None:
        """Creates a tag template if it does not already exist.

        Args:
            parent: The parent resource for the tag template.
        """
        # Create the tag template.
        #self.tag_template = datacatalog_v1.TagTemplate()

        if self.instance_id is None:
            tag_template_name = f"""DLP_columns_{self.project_id}_
            {self.dataset}_{self.table}"""
        else:
            tag_template_name = f"DLP_columns_{self.instance_id}"
        self.tag_template.display_name = tag_template_name

        # Create a new source field for each field in the data.
        fields = {}
        for key, value in self.data[0].items():
            new_source_field = datacatalog_v1.TagTemplateField(
                name=key,
                type=datacatalog_v1.FieldType(
                    primitive_type=datacatalog_v1.FieldType.PrimitiveType.STRING
                ),
                description=value
            )
            fields[new_source_field.name] = new_source_field

        self.tag_template.fields.update(fields)

        # Create the request and send it to create the tag template.
        request = datacatalog_v1.CreateTagTemplateRequest(
            parent=parent,
            tag_template_id=self.tag_template_id,
            tag_template=self.tag_template)

        try:
            self.tag_template = self.client.create_tag_template(request)
        except Exception as e:
            print("Error occured while creating tag template:", str(e))


    def attach_tag_to_table(self, table_entry: str) -> None:
        """Attaches a tag to a BigQuery or CloudSQL table.

        Args:
            table_entry: The table name for the tag to be attached.
        """
        # Attach a tag to the table.
        tag = datacatalog_v1.types.Tag(
            template=self.tag_template.name,
            name="DLP_Analysis")

        for key, value in self.data[0].items():
            tag.fields[key] = datacatalog_v1.types.TagField(string_value=value)

        self.client.create_tag(
            parent=table_entry,
              tag=tag
              )

    def create_custom_entry_group(self) -> str:
        """ Creates a new Custom entry group.
        
            Returns:
                str: The entry_group object name
        """
        entry_group_obj =  datacatalog_v1.types.EntryGroup()
        entry_group_obj.display_name = f"Cloud SQL {self.instance_id}"

        entry_group = self.client.create_entry_group(
            parent= datacatalog_v1.DataCatalogClient.common_location_path(
                self.project_id, self.location
            ),
            entry_group_id=self.entry_group_id,
            entry_group=entry_group_obj,
        )
        return entry_group.name

    def create_entry(self, entry_group_name: str) -> None:
        """ Creates one entry for each column in the inspected table.
        
            Saves the name of the column and the inspection InfoType as the
            description in a new entry that belongs to a entry group.

            Args:
               entry_group_name(str): The complete entry group resurce name.
        """

        # Create an entry
        for data_row in self.data:
            entry = datacatalog_v1.types.Entry()
            entry.user_specified_system = "Cloud_SQL"
            entry.user_specified_type = "SQL"
            entry.display_name = f"DLP_inspection_{self.instance_id}"
            entry.description = ""
            entry.linked_resource =(
                    f"//sqladmin.googleapis.com/projects/{self.project_id}"
                    f"/instances/{self.instance_id}"
                )

        for key, value in data_row.items():
            entry.schema.columns.append(
                datacatalog_v1.types.ColumnSchema(
                    column=key,
                    type_="STRING",
                    description=value,
                    mode=None,
                )
            )

        entry = self.client.create_entry(
            parent=entry_group_name, entry_id=self.entry_id, entry=entry
        )

    def main(self) -> None:
        """Creates a tag template for BigQuery tables and creates custom
            entries for Cloud SQL.
        """

        parent = f"projects/{self.project_id}/locations/{self.location}"

        #should be change after nested tables functinoallity is added
        record_type = None

        # Checks if it's BigQuery or CloudSQL.
        if self.instance_id is None:

            if record_type is None:
                # Create the tag template.
                self.create_tag_template(parent)

            else:
                raise NotImplementedError

            # Creates the BigQuery table entry.
            resource_name = (
                f"//bigquery.googleapis.com/projects/{self.project_id}"
                f"/datasets/{self.dataset}/tables/{self.table}"
                )
            table_entry = self.client.lookup_entry(
            request={"linked_resource": resource_name}
            )
            table_entry = table_entry.name
            # Attach the tag template to the BigQuery table.
            self.attach_tag_to_table(table_entry)

        else:
            entry_group_name = self.create_custom_entry_group()
            self.create_entry(entry_group_name)
