from google.cloud import bigquery
import json
import io
import config

#Inicialización
def init_bq_client(): 
    global bq_client
    bq_client = bigquery.Client()

#Obtención de los nombres de las columnas
def get_headers():
    #Obtención de esquema json de la tabla BQ
    table_bq = bq_client.get_table(config.TABLE_ID)
    f = io.StringIO("")
    bq_client.schema_to_json(table_bq.schema, f)
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

#Obtención de las filas    
def get_rows():
    #Consulta de datos desde BQ para ingresar a DLP
    sqlQuery = "SELECT * from " + "`" + config.DATASET + "`" + "." + "`" + config.TABLE + "` LIMIT 300" 
    query_job = bq_client.query(sqlQuery)
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


def get_item(headers,rows):
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

def get_results():
     init_bq_client()
     headers = get_headers()
     rows = get_rows()
     item = get_item(headers,rows)
     return item