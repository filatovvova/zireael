# Торжественно клянусь, что затеваю только шалость

from sqlite_db import *


def db_create_table(table_name, fields, db_type, db_name='none', conn='none'):
    if db_type == 'sqlite':
        resp = sqlite_create_table(table_name, fields, db_name, conn)
        return resp
