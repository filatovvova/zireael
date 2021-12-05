import yaml
import logging.config
from zireael import *
from configs import const


logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')

with open(const.sqlite_conf_path, 'r') as file:
    db_conf = yaml.safe_load(file)

conn = sqlite3.connect(db_conf['db']['test']['conn_string'])


fields = """fill text\n"""
data_set = [['first'], ['second'], ['third']]
table_name = 'table_fill_2'
pattern = '(?)'
db_type = 'sqlite'
db_name = 'test'
where_block = "1 = 1"

# result = db_create_table(table_name='table_fill_2', fields=fields, db_type='sqlite', db_name='test', if_not_exists=True)
# print(result)

result = db_bulk_insert(table_name=table_name, data_set=data_set, pattern=pattern, db_type=db_type, conn=conn)
print(result)

result = db_delete(table_name=table_name, where_block=where_block, db_type=db_type, conn=conn)
print(result)
