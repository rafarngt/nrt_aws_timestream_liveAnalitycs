import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Sample data
data = [
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407543, "IdPOSLogHeader": 16897, "TxNumber": 6, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390182000, "ReceivedTs": 1719390190867, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407544, "IdPOSLogHeader": 16962, "TxNumber": 15, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390160000, "ReceivedTs": 1719390191283, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407545, "IdPOSLogHeader": 16953, "TxNumber": 7, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390179000, "ReceivedTs": 1719390192357, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407546, "IdPOSLogHeader": 16971, "TxNumber": 18, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390164000, "ReceivedTs": 1719390193483, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407547, "IdPOSLogHeader": 16904, "TxNumber": 7, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390106000, "ReceivedTs": 1719390194090, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407548, "IdPOSLogHeader": 17015, "TxNumber": 7, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719389994000, "ReceivedTs": 1719390194663, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407549, "IdPOSLogHeader": 16888, "TxNumber": 1, "TxCode": "1", "StreamPosition": 0, "SendTs": 1719390193000, "ReceivedTs": 1719390194927, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407550, "IdPOSLogHeader": 16936, "TxNumber": 8, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719389490000, "ReceivedTs": 1719390194997, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:15.576000", "IdPOSLogDetail": 20407551, "IdPOSLogHeader": 16905, "TxNumber": 12, "TxCode": "33", "StreamPosition": 0, "SendTs": 1719390192000, "ReceivedTs": 1719390195453, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "I", "OpDate": "2024-06-26 11:23:16.703000", "IdPOSLogDetail": 20407552, "IdPOSLogHeader": 16961, "TxNumber": 10, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390141000, "ReceivedTs": 1719390196700, "ProcessedTs": None, "IdBCVProcessInfo": None, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:17.026000", "IdPOSLogDetail": 20407541, "IdPOSLogHeader": 16973, "TxNumber": 9, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390143000, "ReceivedTs": 1719390190537, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 3, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:17.056000", "IdPOSLogDetail": 20407541, "IdPOSLogHeader": 16973, "TxNumber": 9, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390143000, "ReceivedTs": 1719390190537, "ProcessedTs": 1719390197053, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 4, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:17.060000", "IdPOSLogDetail": 20407541, "IdPOSLogHeader": 16973, "TxNumber": 9, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390143000, "ReceivedTs": 1719390190537, "ProcessedTs": 1719390197057, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 4, "DepurationProcessId": None},
    {"Op": "I", "OpDate": "2024-06-26 11:23:17.386000", "IdPOSLogDetail": 20407553, "IdPOSLogHeader": 16939, "TxNumber": 8, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390109000, "ReceivedTs": 1719390197380, "ProcessedTs": None, "IdBCVProcessInfo": None, "IdPOSLogDetailStatus": 1, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:17.513000", "IdPOSLogDetail": 20407542, "IdPOSLogHeader": 16906, "TxNumber": 9, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719389996000, "ReceivedTs": 1719390190703, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 3, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:17.553000", "IdPOSLogDetail": 20407542, "IdPOSLogHeader": 16906, "TxNumber": 9, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719389996000, "ReceivedTs": 1719390190703, "ProcessedTs": 1719390197550, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 4, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:17.553000", "IdPOSLogDetail": 20407542, "IdPOSLogHeader": 16906, "TxNumber": 9, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719389996000, "ReceivedTs": 1719390190703, "ProcessedTs": 1719390197550, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 4, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:17.983000", "IdPOSLogDetail": 20407543, "IdPOSLogHeader": 16897, "TxNumber": 6, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390182000, "ReceivedTs": 1719390190867, "ProcessedTs": None, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 3, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:18.020000", "IdPOSLogDetail": 20407543, "IdPOSLogHeader": 16897, "TxNumber": 6, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390182000, "ReceivedTs": 1719390190867, "ProcessedTs": 1719390198017, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 4, "DepurationProcessId": None},
    {"Op": "U", "OpDate": "2024-06-26 11:23:18.020000", "IdPOSLogDetail": 20407543, "IdPOSLogHeader": 16897, "TxNumber": 6, "TxCode": "0", "StreamPosition": 0, "SendTs": 1719390182000, "ReceivedTs": 1719390190867, "ProcessedTs": 1719390198017, "IdBCVProcessInfo": 1826906, "IdPOSLogDetailStatus": 4, "DepurationProcessId": None}
]

# Convert to DataFrame
df = pd.DataFrame(data)

# Convert to PyArrow table
table = pa.Table.from_pandas(df)

# Save to Parquet
pq.write_table(table, 'data_dummy/bcvposlogdetail/20240702-16330000.parquet')
