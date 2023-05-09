# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Createa a Dataplex catalog."""

import google.cloud.bigquery as bq
import google.cloud.datacatalog_v1 as dc


class catalog:
    """
    
    """
    def __init__(self, table: Dict, tag_template_id: str, display_name: str):
        """
        
        """
        
        #hay que agreagr el tag templaete id al run
        # hay que agregar el display name al run
        self.DataCatalogClient = datacatalog_v1.DataCatalogClient()
        self.table = table
        self.tag_template_id = tag_template_id
        self.display_name = display_name
        
    def CreateTag(self):
    """
    
    """
        # Create a TagTemplate object.
        tag_template = DataCatalogClient.TagTemplate()
        tag_template.display_name = self.display_nmae
        tag_template.fields["source_field"].type = datacatalog_v1.FieldType.PrimitiveType.STRING

        # Create the tag template.
        DataCatalogClient.create_tag_template(tag_template)

        # Create a TagEntry object for each entry in the dict.
        for entry_name, entry_value in table.items():
            tag_entry = datacatalog_v1.TagEntry()
            tag_entry.tag_template_id = self.tag_template_id
            tag_entry.value = entry_value

        # Create the tag entries.
        client.create_tag_entries([tag_entry])
        
        return "the job is finished"
       