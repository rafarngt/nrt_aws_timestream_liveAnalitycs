import json
import boto3
from datetime import datetime
import base64
import logging

# Inicializar clientes de AWS
timestream_write = boto3.client('timestream-write')
s3 = boto3.client('s3')

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Definir bases de datos y tablas
DATABASE_NAME = 'my_timestream_db'
TABLES = {
    'bcvposlogdetail': 'bcvposlogdetail_table',
    'bcvposlogtransaction': 'bcvposlogtransaction_table',
    'bcvposlogheader': 'bcvposlogheader_table'
}

def lambda_handler(event, context):
    for record in event['Records']:

         # Decodificar los datos de Kinesis
        payload_data = base64.b64decode(record['kinesis']['data'])
        payload_str = payload_data.decode('utf-8')
        logger.info(f"Decoded payload: {payload_str}")

        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError: {e}")
            continue

        record_type = payload.pop('recordType', None)
        if not record_type:
            logger.error("No recordType found in payload")
            continue

        if record_type == 'bcvposlogdetail':
            upsert_record(payload, TABLES['bcvposlogdetail'])
        elif record_type == 'bcvposlogtransaction':
            upsert_record(payload, TABLES['bcvposlogtransaction'])
        elif record_type == 'bcvposlogheader':
            upsert_record(payload, TABLES['bcvposlogheader'])
        else:
            logger.error(f"Unknown recordType: {record_type}")

    return 'Processed {} records.'.format(len(event['Records']))

def upsert_record(payload, table_name):
    # Convertir fecha a timestamp
    op_date = datetime.strptime(payload['OpDate'], '%Y-%m-%d %H:%M:%S.%f')
    current_time = str(int(op_date.timestamp() * 1000))
    
    # Determinar dimensiones y medidas en función del tipo de registro
    if table_name == 'bcvposlogdetail_table':
        dimensions = [
            {'Name': 'IdPOSLogDetail', 'Value': str(payload['IdPOSLogDetail'])},
            {'Name': 'IdPOSLogHeader', 'Value': str(payload['IdPOSLogHeader'])},
            {'Name': 'TxNumber', 'Value': str(payload['TxNumber'])},
            {'Name': 'TxCode', 'Value': str(payload['TxCode'])},
            {'Name': 'StreamPosition', 'Value': str(payload['StreamPosition'])},
            {'Name': 'IdBCVProcessInfo', 'Value': str(payload['IdBCVProcessInfo'])},
            {'Name': 'IdPOSLogDetailStatus', 'Value': str(payload['IdPOSLogDetailStatus'])},
            {'Name': 'DepurationProcessId', 'Value': str(payload['DepurationProcessId'])}
        ]
        measures = [
            {
                'MeasureName': 'SendTs',
                'MeasureValue': str(payload['SendTs']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'ReceivedTs',
                'MeasureValue': str(payload['ReceivedTs']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'ProcessedTs',
                'MeasureValue': str(payload['ProcessedTs']),
                'MeasureValueType': 'BIGINT'
            }
        ]
    elif table_name == 'bcvposlogtransaction_table':
        dimensions = [
            {'Name': 'ID', 'Value': str(payload['ID'])},
            {'Name': 'IdPOSLogDetail', 'Value': str(payload['IdPOSLogDetail'])},
            {'Name': 'RetailStoreID', 'Value': str(payload['RetailStoreID'])},
            {'Name': 'WorkstationID', 'Value': str(payload['WorkstationID'])},
            {'Name': 'SequenceNumber', 'Value': str(payload['SequenceNumber'])},
            {'Name': 'EmployeeID', 'Value': str(payload['EmployeeID'])},
            {'Name': 'OperatorID', 'Value': str(payload['OperatorID'])},
            {'Name': 'CurrencyCode', 'Value': payload['CurrencyCode']},
            {'Name': 'ActionCode', 'Value': str(payload['ActionCode'])},
            {'Name': 'SubCode', 'Value': str(payload['SubCode'])},
            {'Name': 'CashierName', 'Value': payload['CashierName']},
            {'Name': 'SAPStore', 'Value': payload['SAPStore']}
        ]
        measures = [
            {
                'MeasureName': 'BeginDateTime',
                'MeasureValue': str(payload['BeginDateTime']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'EndDateTime',
                'MeasureValue': str(payload['EndDateTime']),
                'MeasureValueType': 'BIGINT'
            }
        ]
    elif table_name == 'bcvposlogheader_table':
        dimensions = [
            {'Name': 'IdPOSLogHeader', 'Value': str(payload['IdPOSLogHeader'])},
            {'Name': 'IdSucursal', 'Value': str(payload['IdSucursal'])},
            {'Name': 'IdPOSLogHeaderStatus', 'Value': str(payload['IdPOSLogHeaderStatus'])}
        ]
        measures = [
            {
                'MeasureName': 'POSLogBusinessDate',
                'MeasureValue': str(payload['POSLogBusinessDate']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'POSLogEndOfBusinessDate',
                'MeasureValue': str(payload['POSLogEndOfBusinessDate']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'StartProcessTs',
                'MeasureValue': str(payload['StartProcessTs']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'EndProcessTs',
                'MeasureValue': str(payload['EndProcessTs']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'ValidatedTxQty',
                'MeasureValue': str(payload['ValidatedTxQty']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'TotalTxQty',
                'MeasureValue': str(payload['TotalTxQty']),
                'MeasureValueType': 'BIGINT'
            },
            {
                'MeasureName': 'CloseProcessTs',
                'MeasureValue': str(payload['CloseProcessTs']),
                'MeasureValueType': 'BIGINT'
            }
        ]
    
    try:
        # Escribir en Timestream
        timestream_write.write_records(
            DatabaseName=DATABASE_NAME,
            TableName=table_name,
            Records=[
                {
                    'Dimensions': dimensions,
                    'MeasureName': measures[0]['MeasureName'],
                    'MeasureValue': measures[0]['MeasureValue'],
                    'MeasureValueType': measures[0]['MeasureValueType'],
                    'Time': current_time
                }
            ]
        )
        print(f"Upsert successful for record in table {table_name}")

        # Guardar en S3
        #s3.put_object(
        #    Bucket='my-s3-bucket',
        #    Key=f"raw/{current_time}.json",
        #    Body=json.dumps(payload)
        #)

    except Exception as e:
        print(f"Error during upsert: {e}")

