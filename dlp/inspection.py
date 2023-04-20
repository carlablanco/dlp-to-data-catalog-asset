# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs the DLP inspection over the preprocessed table."""

from typing import List, Dict
from google.cloud import dlp_v2

class DlpInspection:
    """Performs a DLP inspection on a preprocessed table to identify
            sensitive information."""
    def __init__(self, project_id: str, language_code: str, tables: List[dlp_v2.Table]):
        """Initializes the class with the required data.

        Args:
            project_id: The project ID to be used.
            language_code: The BCP-47 language code to use, e.g. 'en-US'.
            tables: Tables to be inspected in the correct format.
        """
        self.dlp_client = dlp_v2.DlpServiceClient()
        self.project_id = project_id
        self.language_code = language_code
        self.tables = tables

    def get_inspection_parameters(self):
        """Gets the table to be inspected with an API call.

            Args:
                table: The particular table to be inspected in the correct 
                        format.

            Returns:
                parent: The project route in GCP.
                inspect_config: The configuration for the inspection.
        """
        info_types = self.dlp_client.list_info_types(
                        request={"language_code": self.language_code})
        info_types_names = [
                        info_type.name for info_type in info_types.info_types
                        if self.language_code in info_type.name
                        ]
        inspect_config = {
            "info_types": [{"name": name} for name in info_types_names],
            "min_likelihood": dlp_v2.Likelihood.POSSIBLE
        }
        parent = f"projects/{self.project_id}"
        return parent, inspect_config

    def analyze_inspection_result(self, table_inspected: Dict) -> Dict:
        """Processes the results of the inspection.

            This code iterates through an API response and constructs a
            dictionary.
            Each entry in the dictionary is associated with a column and
            contains a sub-dictionary for each infotype found in the response.
            In each sub-dictionary, the variable name is used as the key
            and the associated value is the likelihood.

            Args:
                table_inspected: The API response to be analyzed.

            Returns:
                finding_results: For every variable there is a dictionary with
                    the infotype and the likelihood value.
                Example: {'name': {'PERSON_NAME': 4.4}, 'age': {'AGE': 5.8}}
        """
        value_likelihood = {
            "POSSIBLE":1,
            "LIKELY":1.2,
            "VERY_LIKELY":1.4
        }
        finding_results = {}
        if table_inspected.result.findings:
            for finding in table_inspected.result.findings:
                try:
                    column = finding.location.content_locations[
                        0].record_location.field_id.name
                    infotypes = finding_results.setdefault(column, {})
                    likelihood = value_likelihood.get(finding.likelihood.name,
                                                         0)
                    # If the infotype is already in the dictionary, sum
                    # the likelihood value to the exisiting one.
                    if finding.info_type.name in infotypes:
                        infotypes[finding.info_type.name] += likelihood
                    else:
                        # If the infotype is not in the dictionary, add it with
                        # the likelihood value.
                        infotypes[finding.info_type.name] = likelihood
                except AttributeError as err:
                    raise ValueError("""AttributeError: No findings
                                        returned from API call.""") from err
        return finding_results

    def get_max_infotype(self, finding_results: Dict) -> Dict:
        """Gets the max infotype for each variable.

            Iterates over the finding results and returns the infotype with
            the highest likelihood.

            Args:
                finding_results: The findings result to be analyzed.

            Returns:
            top_findings: A dictionary where each variable has its respective
              "infotype" and "likelihood value."
        """
        top_findings = {}
        for column in finding_results:
            max_infotype = None
            max_count = 0
            for infotype, count in finding_results.get(column).items():
                # Add the infotype to the top_findings dictionary.
                # If the infotype is already in the dictionary, sum the
                # likelihood value to the existing one, otherwise add it
                # with the likelihood value.
                if max_infotype is None or count > max_count:
                    max_infotype = infotype
                    max_count = count
            top_findings[column] = max_infotype
        return top_findings

    def main(self):
        """Iterates over the given tables and analyzes each one.
            
           Returns:
                results: A list of dictionaries with the infotype with the
                    highest likelihood.
                    Example: [{'name': 'PERSON_NAME', 'age': 'AGE'},
                     {'DNI': 'GOVERMENT_ID', 'token': 'AUTH_TOKEN'}]"""
        results = []
        parent, inspect_config = self.get_inspection_parameters()
        for table in self.tables:
            # Get table to be inspected.
            response = self.dlp_client.inspect_content(
                request={"parent": parent, "item": {"table" : table},
                            "inspect_config": inspect_config})
            # Processes the results of the inspection.
            finding_results = self.analyze_inspection_result(response)
            # Get the max infotype for each variable.
            top_findings = self.get_max_infotype(finding_results)
            # Append to the results list.
            results.append(top_findings)
        return results
