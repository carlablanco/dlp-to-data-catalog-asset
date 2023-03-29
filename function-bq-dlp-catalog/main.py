from bq import BigQuerySchemaRows
from dlp import DlpInspect
from pprint import pprint

def main(request):

    bq_instance = BigQuerySchemaRows()
    item = bq_instance.get_results_to_dlp()
    pprint (item)

    dlp_instance = DlpInspect(item)
    top_findings = dlp_instance.get_results_to_catalog()
    pprint(top_findings)

  
    return "Execution ok"