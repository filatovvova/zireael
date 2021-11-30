import sqlite3

def sqlite_create_table(table_name, fields, db_name, conn):
    if conn != 'none':
