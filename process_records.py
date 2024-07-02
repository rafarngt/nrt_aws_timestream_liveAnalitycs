import json
import boto3
import base64
import logging
from datetime import datetime
import time

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

DEFAULT_INT_VALUE = 0  # Valor por defecto si el valor es None

def _print_rejected_records_exceptions(err):
    logger.error(f"RejectedRecords: {err}")
    for rr in err.response["RejectedRecords"]:
        recordIndex=rr['RecordIndex']
        reason=rr['Reason']
        logger.error(f"Rejected Index {recordIndex} due to {reason}")
        if "ExistingVersion" in rr:
            existingVersion = rr["ExistingVersion"]
            logger.error(f"Rejected record existing version: {existingVersion}")

def _current_milli_time():
    return str(int(round(time.time() * 1000)))

def write_records(table_name, records):
    # Escribir registros en la tabla
    try:
        result= timestream_write.write_records(
            DatabaseName=DATABASE_NAME,
            TableName=table_name,
            Records=records,
            CommonAttributes={}
        )
        logger.info(f"Upsert successful for record in table {table_name}")
        logger.info(f"WriteRecords Status:  { result['ResponseMetadata']['HTTPStatusCode']}")

    except timestream_write.exceptions.RejectedRecordsException as e:
        _print_rejected_records_exceptions(e)
    except Exception as e:
        logger.error(f"Error during upsert: {e}")

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

def convert_to_int(value, default=DEFAULT_INT_VALUE):
    if value is None:
        return default
    if isinstance(value, str):
        try:
            # Asumir que el formato es una fecha/hora y convertirla a timestamp
            dt = datetime.fromisoformat(value)
            tmp = int(dt.timestamp())
            logger.info(f"Decoded payload: {tmp}")
            return int(dt.timestamp())
        except ValueError:
            logger.error(f"Error converting {value} to timestamp")
            return default
    return int(value)

def upsert_record(payload, table_name):
    # Convertir fecha a timestamp
    try:
        op_date = datetime.strptime(payload['OpDate'], '%Y-%m-%d %H:%M:%S.%f')
        op_date_time = str(int(op_date.timestamp() * 1000))
        
    except KeyError as e:
        logger.error(f"Missing key in payload: {e}")
        return
    except ValueError as e:
        logger.error(f"Date conversion error: {e}")
        return
    
    # Determinar dimensiones y medidas en función del tipo de registro
    dimensions = []
    measures = []
    
    current_timestamp = _current_milli_time()
    logger.info(f"Timestamp: {current_timestamp}")

    if table_name == 'bcvposlogdetail_table':
        dimensions = [
            {'Name': 'IdPOSLogDetail', 'Value': str(payload['IdPOSLogDetail'])},
            {'Name': 'IdPOSLogHeader', 'Value': str(payload['IdPOSLogHeader'])},
            {'Name': 'TxNumber', 'Value': str(payload['TxNumber'])},
            {'Name': 'TxCode', 'Value': str(payload['TxCode'])},
            {'Name': 'StreamPosition', 'Value': str(payload['StreamPosition'])},
            {'Name': 'IdBCVProcessInfo', 'Value': str(payload['IdBCVProcessInfo'])},
            {'Name': 'IdPOSLogDetailStatus', 'Value': str(payload['IdPOSLogDetailStatus'])},
            {'Name': 'DepurationProcessId', 'Value': str(payload['DepurationProcessId'])},

        ]
        
        records = [
            {
                'Dimensions': dimensions,
                'MeasureName': 'OpDate', 
                'MeasureValue': op_date_time, 
                'MeasureValueType': 'BIGINT',
                'Time': current_timestamp
            },{
                'Dimensions': dimensions,
                'MeasureName': 'SendTs', 
                'MeasureValue': str(convert_to_int(payload.get('SendTs'))), 
                'MeasureValueType': 'BIGINT',
                'Time': current_timestamp
            },{
                'Dimensions': dimensions,
                'MeasureName': 'ReceivedTs', 
                'MeasureValue': str(convert_to_int(payload.get('ReceivedTs'))), 
                'MeasureValueType': 'BIGINT',
                'Time': current_timestamp
            },{
                'Dimensions': dimensions,
                'MeasureName': 'ProcessedTs', 
                'MeasureValue': str(convert_to_int(payload.get('ProcessedTs'))), 
                'MeasureValueType': 'BIGINT',
                'Time': current_timestamp
            }
        ]
        logger.info(f"measures: {records}")
        write_records(table_name, records)

        
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
            {'MeasureName': 'BeginDateTime', 'MeasureValue': str(convert_to_int(payload.get('BeginDateTime'))), 'MeasureValueType': 'BIGINT'},
            {'MeasureName': 'EndDateTime', 'MeasureValue': str(convert_to_int(payload.get('EndDateTime'))), 'MeasureValueType': 'BIGINT'}
        ]
    elif table_name == 'bcvposlogheader_table':
        dimensions = [
            {'Name': 'IdPOSLogHeader', 'Value': str(payload['IdPOSLogHeader'])},
            {'Name': 'IdSucursal', 'Value': str(payload['IdSucursal'])},
            {'Name': 'IdPOSLogHeaderStatus', 'Value': str(payload['IdPOSLogHeaderStatus'])}
        ]
        measures = [
            {'MeasureName': 'POSLogBusinessDate', 'MeasureValue': str(convert_to_int(payload.get('POSLogBusinessDate'))), 'MeasureValueType': 'BIGINT'},
            {'MeasureName': 'POSLogEndOfBusinessDate', 'MeasureValue': str(convert_to_int(payload.get('POSLogEndOfBusinessDate'))), 'MeasureValueType': 'BIGINT'},
            {'MeasureName': 'StartProcessTs', 'MeasureValue': str(convert_to_int(payload.get('StartProcessTs'))), 'MeasureValueType': 'BIGINT'},
            {'MeasureName': 'EndProcessTs', 'MeasureValue': str(convert_to_int(payload.get('EndProcessTs'))), 'MeasureValueType': 'BIGINT'},
            {'MeasureName': 'ValidatedTxQty', 'MeasureValue': str(convert_to_int(payload.get('ValidatedTxQty'))), 'MeasureValueType': 'BIGINT'},
            {'MeasureName': 'TotalTxQty', 'MeasureValue': str(convert_to_int(payload.get('TotalTxQty'))), 'MeasureValueType': 'BIGINT'},
            {'MeasureName': 'CloseProcessTs', 'MeasureValue': str(convert_to_int(payload.get('CloseProcessTs'))), 'MeasureValueType': 'BIGINT'}
        ]

