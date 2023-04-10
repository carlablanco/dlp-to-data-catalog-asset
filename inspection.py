# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs the DLP inspection over the preprocessed table.
"""

from google.cloud import dlp_v2

class inspection:
    "Class for inspecting the table with the DLP API."
    def __init__(self, language_code: str, item: dict):
        """
        Args:
            language_code: The BCP-47 language code to use, e.g. 'en-US'.
            item: The table to be inspected.
        """        
        self.dlp_client = dlp_v2.DlpServiceClient()
        self.language_code = language_code
        self.item = item 
    
    def get_response(self):
        """API call for inspecting the content on the table.
        
        Args:
           language_code: The BCP-47 language code to use, e.g. 'en-US'.
        """
        self.response = self.dlp_client.inspect_content(
        request={"parent": self.language_code, "item": self.item}
        )
        
    def finding_results(self) -> dict:
        """In this section we are arranging the finding results
            in a new dictionary that counts the appearances of each infotype
            and maps them with their respective columns.

            Returns:
                finding_results: A dictionary with the column name as key and a
                dictionary with the infotype as key and the count as value.
        """
        finding_results = {}
        if self.response.result.findings:
            for finding in self.response.result.findings:
                try:
                    column = finding.location.content_locations[0].record_location.field_id.name

                    if column in finding_results:
                        aux_infotypes = finding_results[column]
                        if finding.info_type.name in aux_infotypes:
                            aux_infotypes[finding.info_type.name] = aux_infotypes[finding.info_type.name] + 1
                        else:
                            aux_infotypes[finding.info_type.name] = 1
                    else:
                        finding_results[column] = {}
                        aux_infotypes = finding_results[column]
                        aux_infotypes[finding.info_type.name] = 1
                except AttributeError:
                    pass
        else:
            print("No findings.")
        return finding_results
    
    
    def max_infotype(self) -> dict:
        """ Get max infotype.
            Need to keep only the the top infotype to add to the data catalog.

            Returns:
                top_findings: A dictionary with the column name as key and the
                top infotype as value.
            """
        top_findings = {}
        for column in self.finding_results:
            max_infotype = None
            max_count = 0
            for it, cnt in self.finding_results.get(column).items():
                if max_infotype is None or cnt > max_count:
                    max_infotype = it
                    max_count = cnt
            top_findings[column] = max_infotype
        print(top_findings)
        return top_findings
