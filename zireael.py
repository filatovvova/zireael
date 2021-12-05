# Торжественно клянусь, что затеваю только шалость

from sqlite_db import *


def db_create_table(table_name, fields, db_type, db_name='none', conn='none', if_not_exists=False):
    if db_type == 'sqlite':
        resp = sqlite_create_table(table_name, fields, db_name, conn, if_not_exists)
        return resp


def db_bulk_insert(table_name, data_set, pattern, db_type, db_name='none', conn='none'):
    if db_type == 'sqlite':
        resp = sqlite_bulk_insert(table_name, data_set, pattern, db_name, conn)
        return resp


def db_delete(table_name, where_block, db_type, db_name='none', conn='none'):
    if db_type == 'sqlite':
        resp = sqlite_delete(table_name, where_block, db_name, conn)
        return resp
