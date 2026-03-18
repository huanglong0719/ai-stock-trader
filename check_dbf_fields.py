
from dbfread import DBF
import os

dbf_path = r"D:\tdxkxgzhb\T0002\hq_cache\base.dbf"
if os.path.exists(dbf_path):
    table = DBF(dbf_path, encoding='gbk')
    print(f"Fields in base.dbf: {table.field_names}")
    # Print first record to see values
    for record in table:
        print(f"Example record: {record}")
        break
else:
    print("File not found")
