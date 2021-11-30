import sqlite3
import logging.config
import yaml
from configs import const

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


def sqlite_create_table(table_name, fields, db_name, conn):
    script = 'create table {}\n (\n  {} )'.format(table_name, fields)
    if conn == 'none':
        logger.info('start create table {} in sqlite db = {}'.format(table_name, db_name))
        cur = None
        conn = None
        try:
            with open(const.sqlite_conf_path, 'r') as file:
                db_conf = yaml.safe_load(file)

            conn = sqlite3.connect(db_conf['db'][db_name]['conn_string'])
            cur = conn.cursor()

            logger.debug('start execute table creation:\n{}'.format(script))
            cur.execute(script)
        except FileNotFoundError as er_message:
            logger.error('Ups, error during table creation: {}'.format(er_message))
            return -1
        except sqlite3.Error as er_message:
            logger.error('Ups, error during table creation: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute table creation')
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
        logger.info('end create table {} in sqlite db = {}'.format(table_name, db_name))
        return 1

    else:
        logger.info('start create table {} sqlite db. Conn is sent to the function'.format(table_name))
        cur = None
        try:
            cur = conn.cursor()

            logger.debug('start execute table creation:\n{}'.format(script))
            cur.execute(script)
        except sqlite3.Error as er_message:
            logger.error('Ups, error during table creation: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute table creation')
        finally:
            if cur:
                cur.close()
        logger.info('end create table {} sqlite db. Conn is sent to the function'.format(table_name))
        return 1

