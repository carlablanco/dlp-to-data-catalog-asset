# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs the DLP inspection over the preprocessed table."""

from typing import Dict
from google.cloud import dlp_v2

class DlpInspection:
    def __init__(self, project_id: str, language_code: str, item: Dict):
        """Initializes the class with the required data.

        Args:
            project_id: The project ID to be used.
            language_code: The BCP-47 language code to use, e.g. 'en-US'.
            item: The table to be inspected in the correct format.
        """
        self.dlp_client = dlp_v2.DlpServiceClient()
        self.project_id = project_id
        self.language_code = language_code
        self.item = item

    def response(self):
        """API call for inspecting the content on the table.

        Returns:
        The response from the API call.
        """
        info_types = self.dlp_client.list_info_types(request={"language_code": self.language_code})
        info_types_names = [info_type.name for info_type in info_types.info_types if
                            (self.language_code in info_type.name)]
        inspect_config = {
            "info_types": [{"name": name} for name in info_types_names],
            "min_likelihood": dlp_v2.Likelihood.POSSIBLE
        }
        parent = f"projects/{self.project_id}"
        response = self.dlp_client.inspect_content(
            request={"parent": parent, "item": self.item, "inspect_config": inspect_config})
        return response

    def finding_results(self) -> Dict:
        """ Get the results of the inspection.
        Creates a dictionary with the column name as key and a dictionary
        with the infotype as key and the likelihood as value.

        Returns:
        finding_results: A dictionary with the column name as key and a
        dictionary with the infotype as key and the count as value.
        """
        response = self.response()
        value_likelihood = {
            "POSSIBLE":1,
            "LIKELY":1.2,
            "VERY_LIKELY":1.4
        }
        finding_results = {}
        if not response or not response.result.findings:
            raise Exception("No findings returned from API call.")
        
        for finding in response.result.findings:
            column = finding.location.content_locations[0].record_location.field_id.name
            if column not in finding_results:
                finding_results[column] = {}

            aux_infotypes = finding_results[column]

            if finding.info_type.name not in aux_infotypes:
                aux_infotypes[finding.info_type.name] = (
                    value_likelihood[finding.likelihood.name])
            elif value_likelihood[finding.likelihood.name] > (
                aux_infotypes[finding.info_type.name]):
                aux_infotypes[finding.info_type.name] = (
                    value_likelihood[finding.likelihood.name])
        return finding_results

    def max_infotype(self) -> Dict:
        """Returns the infotype with the highest likelihood.

        Iterates over the finding results and returns the infotype with
        the highest likelihood.

        Returns:
        top_findings: A dictionary with the column name as key and
        the top infotype as value.
        """
        finding_results = self.finding_results()
        for column in finding_results:
            aux_infotypes = finding_results[column]
            highest_likelihood = max(aux_infotypes.values())
            filtered_infotypes = {}
            for infotype, likelihood in aux_infotypes.items():
                if likelihood == highest_likelihood:
                    filtered_infotypes[infotype] = likelihood
            finding_results[column] = filtered_infotypes
        return finding_results
     