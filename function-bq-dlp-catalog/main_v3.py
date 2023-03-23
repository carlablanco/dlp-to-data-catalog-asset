from bq import get_results
from pprint import pprint

def main(request):

    item = get_results()

    pprint (item)
  
    return "Execution ok"