import boto3


def connect():
    # 1 - Create Client
    ddb = boto3.resource('dynamodb',
                         endpoint_url='http://localhost:8000',
                         region_name='dummy',
                         aws_access_key_id='dummy',
                         aws_secret_access_key='dummy')
    return ddb

def create_table(ddb):
    ddb.create_table(TableName='Transactions',
                     AttributeDefinitions=[
                         {
                             'AttributeName': 'TransactionId',
                             'AttributeType': 'S'
                         }
                     ],
                     KeySchema=[
                         {
                             'AttributeName': 'TransactionId',
                             'KeyType': 'HASH'
                         }
                     ],
                     ProvisionedThroughput= {
                         'ReadCapacityUnits': 10,
                         'WriteCapacityUnits': 10
                     }
                     )
    print('Successfully created Table')


def inset_data(table):
    input = {'TransactionId': '11ewqarsfh9a0', 'State': 'PENDING', 'Amount': 50}
    table.put_item(Item=input)
    print('Successfully put item')


def scan_table(table):
    scanResponse = table.scan(TableName='Transactions')
    items = scanResponse['Items']
    for item in items:
        print(item)
