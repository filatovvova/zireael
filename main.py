import yaml
import logging.config
from zireael import *
from configs import const


logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')

fields = """fill INTEGER\n"""
with open(const.sqlite_conf_path, 'r') as file:
    db_conf = yaml.safe_load(file)

conn = sqlite3.connect(db_conf['db']['test']['conn_string'])

result = db_create_table(table_name='table_fill_3', fields=fields, db_type='sqlite', db_name='test')
print(result)

