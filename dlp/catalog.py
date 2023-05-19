# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"A class that creates and attaches a tag template to a BigQuery table."

from typing import List, Dict, Optional
from google.cloud import datacatalog_v1

class Catalog: 
    def __init__(self, data: List[Dict], tag_template_id: str,
                 project_id: str, location: str,
                 dataset: Optional[str] = None, table: Optional[str] = None,
                 overwrite_catalog:str = False, instance_id: str = None):

        self.data_catalog_client = datacatalog_v1.DataCatalogClient()
        self.data = data
        self.tag_template_id = tag_template_id
        self.project_id = project_id
        self.location = location
        self.dataset = dataset
        self.table = table
        self.overwrite_catalog = True
        self.instance_id = instance_id

    def create_tag_template(self, parent: str) -> None:
        """Creates a tag template if it does not already exist.

        Args:
            parent: The parent resource for the tag template.
        """
        # Create the tag template.
        tag_template = datacatalog_v1.TagTemplate()
        if instance is not None:
            self.tag_template_name = f"DLP_columns_{project_id}_{dataset}_{table}"
        else:
            self.tag_template_name = f"DLP_columns_{instance_id}"
        tag_template.display_name = self.tag_template_name
        # Create a new source field for each field in the data.
        for key, value in self.data[0].items():
            new_source_field = datacatalog_v1.TagTemplateField()
            new_source_field.name = key
            new_source_field.type.primitive_type = (
                datacatalog_v1.FieldType.PrimitiveType.STRING)
            new_source_field.description = value
            tag_template.fields.update({new_source_field.name:(
                new_source_field)})

        # Create the request and send it to create the tag template.
        request = datacatalog_v1.CreateTagTemplateRequest(
            parent=parent, tag_template_id=self.tag_template_id,
            tag_template=tag_template)
        self.tag_template =(
        self.data_catalog_client.create_tag_template(request))
        print(self.tag_template)

    def attach_to_table(self, table_entry: str) -> None:
        """Attaches a tag to a BigQuery table.

        Args:
            table_entry: The table name for the tag to be attached.
        """
        # Attach a tag to the table.
        tag = datacatalog_v1.types.Tag()

        tag.template = self.tag_template.name
        tag.name = "DLP_Analysis"

        for key, value in self.data[0].items():
            tag.fields[key] = datacatalog_v1.types.TagField()
            tag.fields[key].string_value = value

        tag = self.data_catalog_client.create_tag(parent=table_entry, tag=tag)

    def main(self) -> None:
        """Creates a tag template and attaches it to a BigQuery table.

        Returns:
            A string indicating that the job is finished.
        """
        parent = f"projects/{self.project_id}/locations/{self.location}"

        # Create the tag template.
        self.create_tag_template(parent)

        # Attach the tag template to the BigQuery table.

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
