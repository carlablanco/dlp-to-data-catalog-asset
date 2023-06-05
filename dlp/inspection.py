# Copyright 2023 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Runs the DLP inspection over the preprocessed table."""

from typing import List, Dict
from google.cloud import dlp_v2
from google.api_core.exceptions import BadRequest

class DlpInspection:
    """Performs a DLP inspection on a preprocessed table to identify
            sensitive information."""

    def __init__(self, project_id: str, language_code: str,
                 tables: List[dlp_v2.Table]=None):
        """Initializes the class with the required data.

        Args:
            project_id: The project ID to be used.
            language_code: The BCP-47 language code to use, e.g. "en-US".
            tables: Tables to be inspected in the correct format.
        """
        self.dlp_client = dlp_v2.DlpServiceClient()
        self.project_id = project_id
        self.language_code = language_code
        self.tables = tables

    def get_inspection_parameters(self):
        """Gets the table to be inspected with an API call.

            Returns:
                parent (str): The project route in GCP.
                inspect_config (Dict): The configuration for the inspection.
        """
        info_types = self.dlp_client.list_info_types(
            request={"language_code": self.language_code})
        info_types_names = [
            info_type.name for info_type in info_types.info_types
            if self.language_code in info_type.name
        ]
        inspect_config = {
            "info_types": [{"name": name} for name in info_types_names]
        }
        parent = f"projects/{self.project_id}"
        return parent, inspect_config

    def analyze_inspection_result(self, results: List[Dict] ) -> Dict:
        """Processes the results of the inspection.

            This code iterates through a list of API responses and constructs a
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
                Example: {"name": {"PERSON_NAME": 4.4}, "age": {"AGE": 5.8}}
        """

        table_inspected = {}
        # Create a dictionary in the correct format
        # to analyze the API response.
        finding_results = {}
        for result in (results):
            table_inspected["result"] = result.result

            value_likelihood = {
                "LIKELIHOOD_UNSPECIFIED": 1,
                "VERY_UNLIKELY": 0.6,
                "UNLIKELY": 0.8,
                "POSSIBLE": 1,
                "LIKELY": 1.2,
                "VERY_LIKELY": 1.4
            }

            if table_inspected["result"].findings:
                for finding in table_inspected["result"].findings:
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
                            # If the infotype is not in the dictionary,
                            # add it with the likelihood value.
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

    def analyze_dlp_table(self, parent: str, table: str,
                          inspect_config: Dict) -> List[Dict]:
        """ Analyze the complete DLP table in blocks of 10000 cells.

            This function iteratively analyzes a large DLP table by making API
            calls in blocks of 10000 cells at a time. This helps to avoid
            exceeding API quotas and rate limits, which can cause errors
            and delays.

            Args:
               parent (str): The project route in GCP.
               table: The particular table to be inspected in the correct
                        format.
               inspect_config (Dict): Parameters for the ispection. InfoTypes
                               and the minimum likelihood.

            Returns:
                List[Dict]: The response from the API. Each varibale is
                inspected and returns findings for each record.

        """
        # The Block size adecuate to the DLP scan.
        block_size = 10000
        num_headers = len(table.headers)
        # Get the headers from the first row of the table.
        dlp_table = dlp_v2.Table()
        dlp_table.headers = [
            {"name": table.headers[i].name} for i in range(num_headers)]

        # List of data chunks of 10000 cells.
        data_chunks = [
                     table.rows[rows:rows+int((block_size/num_headers))]
                     for rows in range(0, len(table.rows),
                     int((block_size/num_headers)))
                        ]

        # Create a list for the DLP inspections.
        results_list = []

        for chunk in data_chunks:
            # Get specific data chunk.
            chunk_data = [[value.string_value for value in row.values]
                          for row in chunk]

            # Add the specific data chunk to the dlp object.
            rows = []
            for row in chunk_data:
                rows.append(dlp_v2.Table.Row(
                    values=[dlp_v2.Value(
                        string_value=cell_val) for cell_val in row]))

            dlp_table.rows = rows
            try:
                # Make the API request for the chunk of data.
                response = self.dlp_client.inspect_content(
                    request={
                        "parent": parent,
                        "item": {"table": dlp_table},
                        "inspect_config": inspect_config
                    }
                )
                # Append the chunk inspection into the results.
                results_list.append(response)
            except BadRequest as error:
                # Handle the BadRequest exception here.
                raise BadRequest(error) from error
        return results_list

    def get_finding_results(self,table: dlp_v2.Table) -> Dict:
        """Retrieve the finding results of inspected cells in a table.
        This method takes a table and performs data inspection using the
        configured inspection parameters. It returns a dictionary containing
        the finding results for the inspected cells.

            Args:
                table: The particular table to be inspected in the correct
                            format.

           Returns:
                A dictionary, where each variable has its respective
              "infotype" and "likelihood value."""
        parent, inspect_config = self.get_inspection_parameters()

        # Get the complete table inspected.
        results_lists = self.analyze_dlp_table(parent, table,
                                                inspect_config)
        # Processes the results of the inspection.
        finding_results = self.analyze_inspection_result(results_lists)

        return finding_results

    def merge_and_top_finding(self,finding_results_list: List) -> Dict:
        """_summary_

        Args:
            finding_results_list (_type_): _description_

        Returns:
            _type_: _description_
        """
        merge_finding_result = {}

        for finding_results in finding_results_list:
            for key, values in finding_results.items():
                for infotype, value in values.items():
                    if key not in merge_finding_result:
                        merge_finding_result[key] = {}
                    merge_finding_result[key][infotype] =  \
                        merge_finding_result[key].get(infotype, 0) + value

        return self.get_max_infotype(merge_finding_result)
    