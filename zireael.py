# Торжественно клянусь, что затеваю только шалость

from sqlite_db import *
from mysql_db import *
import logging.config

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


def db_create_table(table_name, fields, db_type, db_name='none', conn='none', if_not_exists=False):
    if db_type == 'sqlite':
        resp = sqlite_create_table(table_name, fields, db_name, conn, if_not_exists)
        return resp


def db_bulk_insert(table_name, data_set, values_pattern, fields_name_pattern, db_type, db_name='none', conn='none'):
    if db_type == 'sqlite':
        resp = sqlite_bulk_insert(table_name, data_set, values_pattern, db_name, conn)
        return resp
    elif db_type == 'mysql':
        resp = mysql_bulk_insert(table_name, data_set, fields_name_pattern, values_pattern, db_name, conn)
        return resp
    else:
        logger.error('incorrect db type: {}'.format(db_type))
        return -1


def db_delete(table_name, where_block, db_type, db_name='none', conn='none'):
    if db_type == 'sqlite':
        resp = sqlite_delete(table_name, where_block, db_name, conn)
        return resp


def db_select(script, db_type, db_name='none', conn='none'):
    if db_type == 'sqlite':
        resp, result = sqlite_select(script, db_name, conn)
        return resp, result


def db_execute_with_commit(script, db_type, db_name='none', conn='none'):
    if db_type == 'sqlite':
        resp = sqlite_execute_with_commit(script, db_name, conn)
        return resp
