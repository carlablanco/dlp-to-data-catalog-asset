from google.cloud import dlp_v2
from pprint import pprint
import json

class DlpInspect:
    PROJECT = 'data-poc-sandbox-378818'
    PARENT = f"projects/{PROJECT}"
    
    #Configuración DLP
    VALUE_LIKELIHOOD = {
        "LIKELIHOOD_UNSPECIFIED":1,
        "VERY_UNLIKELY":0.6,
        "UNLIKELY":0.8,
        "POSSIBLE":1,
        "LIKELY":1.5,
        "VERY_LIKELY":1.8
    }

    INSPECT_CONFIG = {
    "min_likelihood": dlp_v2.Likelihood.POSSIBLE #Trae likelihood a partir de POSSIBLE
    }

    def __init__(self,item:dict): 
        """ Class instance initialization

        Args:
            item (dict): input schema for inspection in dlp
            
            item = {
                "table": {
                    "headers":[
                        { "name": "col1"}
                        {"name":"col2" }
                    ],
                    rows: [
                        #row1  { "values": [
                            { "string_value":"row1.value-col1"},
                            {"string_value": "row1.value-col2"}    
                                ]},
                        #row2  { "values": [
                            { "string_value":"row2.value-col1"},
                            {"string_value": "row2.value-col2"}    
                                ]},
                    ]
                }
        }
        """
        
        self.dlp = dlp_v2.DlpServiceClient()
        self.item = item

    def response_inspect(self) -> dict:
        """Inspects the data entered
        
        Returns:
            dict: _description_
        """
        response = self.dlp.inspect_content(
        request={"parent": DlpInspect.PARENT,
                  "item": self.item,
                    "inspect_config": DlpInspect.INSPECT_CONFIG}
        )
        print (json.dumps(self.item))
        return response

    def get_finding_results(self,response: dict) -> dict:
        """According to the infotypes found, it groups them in a dict according to their likelihood weighting

        Args:
            response (dict): result of the inspection

        Returns:
            returns a dictionary with the infotypes found with their weighting
            finding_results = {
                col1:{
                    infotype1:300,
                    infotype2:20
                },
                col2:{
                    infotype1:100
                    infotype3:2.5
                }
            }
        """
        finding_results = {}
        if response.result.findings:
            for finding in response.result.findings:
                try:
                    column = finding.location.content_locations[0].record_location.field_id.name
                    aux_infotypes = finding_results[column] if column in finding_results else {}
                    value = aux_infotypes[finding.info_type.name] if aux_infotypes[finding.info_type.name] else 0
                    aux_infotypes[finding.info_type.name] = value + DlpInspect.VALUE_LIKELIHOOD[finding.likelihood.name]
                except AttributeError:
                    pass

        pprint(finding_results)
        return finding_results

    def get_top_findings(self,finding_results:dict) -> dict:
        """Returns a dict containing the columns with their infotype of highest weighting

        Args:
            finding_results (dict): _description_

        Returns:
            dict: Returns a dict containing the columns with their infotype of highest weighting
            top_findings = { nombre_columna:infotype2,nombre_columna2:infotype1 }
        """
        top_findings = {}
        
        if(finding_results):
            for column in finding_results:
                max_infotype = None
                max_count = 0

                for it, cnt in finding_results.get(column).items():
                    if max_infotype == None or cnt > max_count:
                        max_infotype = it
                        max_count = cnt
                top_findings[column] = max_infotype
            
            return top_findings

    def get_results_to_catalog(self) -> dict:
        """Runs through all methods to obtain top_findings

        Returns:
            dict: Returns a dict containing the columns with their infotype of highest weighting
        """
        response = self.response_inspect()
        finding_results = self.get_finding_results(response)
        top_findings = self.get_top_findings(finding_results)
        return top_findings