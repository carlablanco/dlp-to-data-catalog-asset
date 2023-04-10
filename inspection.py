# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs the DLP inspection over the preprocessed table.
"""

from google.cloud import dlp_v2

class DlpInspection:
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
        inspect_config = {
        "min_likelihood": dlp_v2.Likelihood.POSSIBLE
        }
        response = self.dlp_client.inspect_content(
        request={"parent": self.language_code, "item": self.item,  "inspect_config":inspect_config})
        return response

    def finding_results(self) -> dict:
        """Get the results of the inspection.
            Creates a dictionary with the column name as key and a dictionary
            with the infotype as key and the likelihood as value.

            Returns:
                finding_results: A dictionary with the column name as key and a
                dictionary with the infotype as key and the count as value.
        """
        response = self.get_response()
        value_likelihood = {
        "POSSIBLE":1,
        "LIKELY":1.2,
        "VERY_LIKELY":1.4}
        finding_results = {}
        if response.result.findings:
            for finding in response.result.findings:
                try:
                    column = finding.location.content_locations[0].record_location.field_id.name
                except AttributeError:
                    continue

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
        else:
            print("No findings.")
        return finding_results

    def max_infotype(self) -> dict:
        """ Get max infotype.
            Need to keep only the the highest infotype to add to the data catalog.

            Returns:
                top_findings: A dictionary with the column name as key and the
                top infotype as value.
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
    