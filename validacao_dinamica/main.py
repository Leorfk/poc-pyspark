from repository.Dynamo_locally_poc import *

ddb = connect()
#create_table(ddb)
table = ddb.Table('Transactions')
inset_data(table)
scan_table(table)
print(list(ddb.tables.all()))