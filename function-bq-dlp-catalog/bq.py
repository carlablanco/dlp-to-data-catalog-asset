from google.cloud import bigquery
import json
import io
class BigQuerySchemaRows:
    project = 'data-poc-sandbox-378818'
    dataset  = 'jimedina_pd'
    table = 'pdata'
    table_id = f'{project}.{dataset}.{table}'
    limit = "100"

    def __init__(self,project=None,dataset=None,table=None):
        self.bq_client = bigquery.Client()
        if project and dataset and table: self.table_id = f'{project}.{dataset}.{table}'


    def get_query(self):
        query = "SELECT * "
        #columns_string = ",".join(columns) if columns else "*"
        #query += columns_string
        query += f" FROM `{self.table_id}` "
        limit_string = f"LIMIT {self.limit}" if self.limit else ""
        query += limit_string
        return query

    def get_headers(self):

        #Obtención de esquema json de la tabla BQ
        table_bq = self.bq_client.get_table(self.table_id)
        f = io.StringIO("")
        self.bq_client.schema_to_json(table_bq.schema, f)
        bq_schema_json = json.loads(f.getvalue())
    
        #Mapeo de datos para pasar a DLP el schema
        
        headers = []
        for i in bq_schema_json:
            headers.append({ "name": i['name']})
        
        '''
            headers = [
                { "name": "col1"},
                {"name" : "col2"}

            ]
        '''    
        return headers
    
    def get_rows(self):
        #Consulta de datos desde BQ para ingresar a DLP
        sqlQuery = self.get_query() 
        query_job = self.bq_client.query(sqlQuery)
        bqrows = query_job.result()

        rows = []
        records = [dict(row) for row in bqrows]

        for row in records:
            rows.append({"values": [{"string_value": str(cell_val)} for cell_val in row.values()]}) 

        '''
        rows = [
        #row1  { "values": [
                            { "string_value":"row1.value1"},
                            {"string_value": "row1.value2"}    
            ]},
        #row2  { "values": [
                            { "string_value":"row2.value1"},
                            {"string_value": "row2.value2"}    
            ]},

        ]
        
        '''
        return rows
    
    def get_item(self,headers,rows):
        #Armado del objeto final con los headers y los rows
        table_dlp = {}
        table_dlp["headers"] = headers
        table_dlp["rows"] = rows
        item = {"table": table_dlp}

        '''
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
        
        '''
        return item
    
    def get_results_to_dlp(self):
        headers = self.get_headers()
        rows = self.get_rows()
        item = self.get_item(headers,rows)
        return item



        





