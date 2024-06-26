import boto3
import pyarrow.parquet as pq
import os
import json
import time
import random
from datetime import datetime

# Inicializar cliente de Kinesis
kinesis = boto3.client('kinesis')

# Ruta a la carpeta principal que contiene los archivos Parquet para bcvposlogheader
data_folder = 'data_dummy/bcvposlogheader'

# Nombre del stream de Kinesis
kinesis_stream_name = 'poslog-stream'

def convert_datetime_to_string(record):
    for key, value in record.items():
        if isinstance(value, datetime):
            record[key] = value.isoformat()
    return record

# Función para leer datos de un archivo Parquet y enviarlos a Kinesis
def send_parquet_to_kinesis(file_path, record_type):
    table = pq.read_table(file_path)
    for record in table.to_pylist():
        record['recordType'] = record_type  # Añadir tipo de registro
        print(f"Sending record: {record}")
        partition_key = f"{record_type}-{record['IdPOSLogHeader']}" 
        kinesis.put_record(
            StreamName=kinesis_stream_name,
            Data=json.dumps(record),
            PartitionKey=partition_key
        )
        time.sleep(1)  # Simula un envío cada segundo

# Recorrer archivos Parquet en la carpeta
record_type = 'bcvposlogheader'
for file in os.listdir(data_folder):
    if file.endswith('.parquet'):
        file_path = os.path.join(data_folder, file)
        print(f"Sending data from {file_path}")
        send_parquet_to_kinesis(file_path, record_type)
