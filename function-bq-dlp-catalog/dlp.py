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

    def __init__(self,item):    
        #Instancia DLP
        self.dlp = dlp_v2.DlpServiceClient()
        self.item = item


    def response_inspect(self):
        response = self.dlp.inspect_content(
        request={"parent": DlpInspect.PARENT,
                  "item": self.item,
                    "inspect_config": DlpInspect.INSPECT_CONFIG}
        )
        print (json.dumps(self.item))
        return response

    def get_finding_results(self,response: dict) -> dict: #hay que tipar todas las funciones, especificando tipos de datos de entrada y de retorno de cada función
        """_summary_

        Args:
            response (dict): _description_

        Returns:
            retorna un diccionario re fachero.
        """
        finding_results = {}
        if response.result.findings:
            for finding in response.result.findings:
                try:
                    column = finding.location.content_locations[0].record_location.field_id.name
                    if column in finding_results:
                        aux_infotypes = finding_results[column]
                        if finding.info_type.name in aux_infotypes:
                            aux_infotypes[finding.info_type.name] = aux_infotypes[finding.info_type.name] + DlpInspect.VALUE_LIKELIHOOD[finding.likelihood.name]
                        else:
                            aux_infotypes[finding.info_type.name] = DlpInspect.VALUE_LIKELIHOOD[finding.likelihood.name]
                    else:
                        finding_results[column] = {}
                        aux_infotypes = finding_results[column]
                        aux_infotypes[finding.info_type.name] = DlpInspect.VALUE_LIKELIHOOD[finding.likelihood.name]
                except AttributeError:
                    pass
        
            '''
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
            
            '''

        pprint(finding_results)
        return finding_results

    def get_top_findings(self,finding_results):
        top_findings = {}
        
        #Primero itera sobre las keys
        if(finding_results):
            for column in finding_results:
                max_infotype = None
                max_count = 0

            #Iteración sobre los objetos
                for it, cnt in finding_results.get(column).items():
                    if max_infotype == None or cnt > max_count:
                        max_infotype = it
                        max_count = cnt
                top_findings[column] = max_infotype
            '''
            top_finding = { nombre_columna:infotype2,nombre_columna2:infotype1 }
            '''
            
            return top_findings

    def get_results_to_catalog(self):
        response = self.response_inspect()
        finding_results = self.get_finding_results(response)
        top_findings = self.get_top_findings(finding_results)
        return top_findings




