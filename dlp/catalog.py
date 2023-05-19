# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"Creates and attaches a tag template to a BigQuery table."

from typing import List, Dict, Optional
from google.cloud import datacatalog_v1

class Catalog: 
    def __init__(self, data: List[Dict], tag_template_id: str,
                 project_id: str, location: str,
                 dataset: Optional[str] = None, table: Optional[str] = None,
                 instance_id: 
                 Optional[str] = None):
        """Initializes the class with the required data.

        Args:
            data(str): The data Previously inspected by the DLP API.
            tag_template_id(str): The unique identifier of the tag template.
            project(str): Project ID for which the client acts on behalf of.
            location(str): The compute engine region.
            dataset(str): The BigQuery dataset to be scanned. Optional.
            table(str): The name of the table. Optional.
            instance(str): Name of the database instance. Optional.
                           Default value is None.
        """
        self.data_catalog_client = datacatalog_v1.DataCatalogClient()
        self.data = data
        self.tag_template_id = tag_template_id
        self.project_id = project_id
        self.location = location
        self.dataset = dataset
        self.table = table
        self.instance_id = instance_id

    def create_tag_template(self, parent: str) -> None:
        """Creates a tag template if it does not already exist.

        Args:
            parent: The parent resource for the tag template.
        """
        # Create the tag template.
        tag_template = datacatalog_v1.TagTemplate()
        if instance is not None:
            tag_template_name = f"DLP_columns_{self.project_id}_{self.dataset}_{self.table}"
        else:
            tag_template_name = f"DLP_columns_{self.instance_id}"
        tag_template.display_name = tag_template_name
        # Create a new source field for each field in the data.
        for key, value in self.data[0].items():
            new_source_field = datacatalog_v1.TagTemplateField(
                name=key,
                type=datacatalog_v1.FieldType(
                    primitive_type=datacatalog_v1.FieldType.PrimitiveType.STRING
                ),
                description=value
             )
            tag_template.fields.update(
                {new_source_field.name:(new_source_field)})

        # Create the request and send it to create the tag template.
        request = datacatalog_v1.CreateTagTemplateRequest(
            parent=parent,
            tag_template_id=self.tag_template_id,
            tag_template=tag_template)
        self.tag_template =(
        self.data_catalog_client.create_tag_template(request)
        )

    def attach_to_table(self, table_entry: str) -> None:
        """Attaches a tag to a BigQuery table.

        Args:
            table_entry: The table name for the tag to be attached.
        """
        # Attach a tag to the table.
        tag = datacatalog_v1.types.Tag(
            template=self.tag_template.name,
            name="DLP_Analysis")

        for key, value in self.data[0].items():
            tag.fields[key] = datacatalog_v1.types.TagField(string_value=value)

        self.data_catalog_client.create_tag(parent=table_entry,
                                                  tag=tag)

    def main(self) -> None:
        """Creates a tag template and attaches it to a BigQuery table.

        Returns:
            A string indicating that the job is finished.
        """
        parent = f"projects/{self.project_id}/locations/{self.location}"

        # Create the tag template.
        self.create_tag_template(parent)

        if self.instance_id is None:
            resource_name = (
                f"//bigquery.googleapis.com/projects/{self.project_id}"
                f"/datasets/{self.dataset}/tables/{self.table}"
             )
        else:
            resource_name = (
                f"//sqladmin.googleapis.com/projects/{self.project_id}"
                f"/instances/{self.instance_id}"
             )

        table_entry = self.data_catalog_client.lookup_entry(
            request={"linked_resource": resource_name}
         )
        table_entry = table_entry.name
        self.attach_to_table(table_entry)
