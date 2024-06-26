import boto3
import pyarrow.parquet as pq
import os
import json
import time
import random
from datetime import datetime

# Inicializar cliente de Kinesis
kinesis = boto3.client('kinesis')

# Ruta a la carpeta principal que contiene los archivos Parquet para bcvposlogdetail
data_folder = 'data_dummy/bcvposlogdetail'

# Nombre del stream de Kinesis
kinesis_stream_name = 'poslog-stream'

# Función para convertir datetime a string
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
        record = convert_datetime_to_string(record)  # Convertir datetime a string
        print(f"Sending record: {record}")
        partition_key = f"{record_type}-{record['IdPOSLogDetail']}" 
        kinesis.put_record(
            StreamName=kinesis_stream_name,
            Data=json.dumps(record),
            PartitionKey=partition_key
        )
        time.sleep(1)  # Simula un envío cada segundo

# Recorrer archivos Parquet en la carpeta
record_type = 'bcvposlogdetail'
for file in os.listdir(data_folder):
    if file.endswith('.parquet'):
        file_path = os.path.join(data_folder, file)
        print(f"Sending data from {file_path}")
        send_parquet_to_kinesis(file_path, record_type)
