import sqlite3
import logging.config
import yaml
from configs import const

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


def sqlite_create_table(table_name, fields, db_name, conn, if_not_exists):
    script_template = 'create table{} {}\n (\n  {} )'
    if if_not_exists:
        replace_keyword = ' if not exists'
    else:
        replace_keyword = ''
    script = script_template.format(replace_keyword, table_name, fields)

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
        except yaml.parser.ParserError as er_message:
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


def sqlite_bulk_insert(table_name, data_set, pattern, db_name, conn):
    script = 'insert into {} values {}'.format(table_name, pattern)
    if conn == 'none':
        logger.info('start bulk insert into table {} in sqlite db = {}'.format(table_name, db_name))
        cur = None
        conn = None
        try:
            with open(const.sqlite_conf_path, 'r') as file:
                db_conf = yaml.safe_load(file)

            conn = sqlite3.connect(db_conf['db'][db_name]['conn_string'])
            cur = conn.cursor()

            logger.debug('start execute bulk insert:\n{}'.format(script))
            cur.executemany(script, data_set)
            conn.commit()
        except FileNotFoundError as er_message:
            logger.error('Ups, error during bulk insert: {}'.format(er_message))
            return -1
        except sqlite3.Error as er_message:
            logger.error('Ups, error during bulk insert: {}'.format(er_message))
            return -1
        except KeyError as er_message:
            logger.error('Ups, error during bulk insert: {}'.format(er_message))
            return -1
        except yaml.parser.ParserError as er_message:
            logger.error('Ups, error during bulk insert: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute bulk insert')
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
        logger.info('end bulk insert into table {} in sqlite db = {}'.format(table_name, db_name))
        return 1

    else:
        logger.info('start bulk insert into table {} sqlite db. Conn is sent to the function'.format(table_name))
        cur = None
        try:
            cur = conn.cursor()

            logger.debug('start execute bulk insert:\n{}'.format(script))
            cur.executemany(script, data_set)
            conn.commit()
        except sqlite3.Error as er_message:
            logger.error('Ups, error during bulk insert: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute bulk insert')
        finally:
            if cur:
                cur.close()
        logger.info('end bulk insert into table {} sqlite db. Conn is sent to the function'.format(table_name))
        return 1


def sqlite_delete(table_name, where_block, db_name, conn):
    script = 'delete from {}\nwhere {}'.format(table_name, where_block)
    if conn == 'none':
        logger.info('start delete from table {} in sqlite db = {}'.format(table_name, db_name))
        cur = None
        conn = None
        try:
            with open(const.sqlite_conf_path, 'r') as file:
                db_conf = yaml.safe_load(file)

            conn = sqlite3.connect(db_conf['db'][db_name]['conn_string'])
            cur = conn.cursor()

            logger.debug('start execute delete:\n{}'.format(script))
            cur.execute(script)
            conn.commit()
        except FileNotFoundError as er_message:
            logger.error('Ups, error during delete: {}'.format(er_message))
            return -1
        except sqlite3.Error as er_message:
            logger.error('Ups, error during delete: {}'.format(er_message))
            return -1
        except KeyError as er_message:
            logger.error('Ups, error during delete: {}'.format(er_message))
            return -1
        except yaml.parser.ParserError as er_message:
            logger.error('Ups, error during delete: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute delete')
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
        logger.info('end delete from table {} in sqlite db = {}'.format(table_name, db_name))
        return 1

    else:
        logger.info('start delete from table {} sqlite db. Conn is sent to the function'.format(table_name))
        cur = None
        try:
            cur = conn.cursor()

            logger.debug('start execute delete:\n{}'.format(script))
            cur.execute(script)
            conn.commit()
        except sqlite3.Error as er_message:
            logger.error('Ups, error during delete: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute delete')
        finally:
            if cur:
                cur.close()
        logger.info('end delete from table {} sqlite db. Conn is sent to the function'.format(table_name))
        return 1


def sqlite_select(script, db_name, conn):
    if conn == 'none':
        logger.info('start select in sqlite db = {}'.format(db_name))
        cur = None
        conn = None
        result = None
        try:
            with open(const.sqlite_conf_path, 'r') as file:
                db_conf = yaml.safe_load(file)

            conn = sqlite3.connect(db_conf['db'][db_name]['conn_string'])
            cur = conn.cursor()

            logger.debug('start execute select:\n{}'.format(script))
            cur.execute(script)
            result = cur.fetchall()
        except FileNotFoundError as er_message:
            logger.error('Ups, error during select: {}'.format(er_message))
            return -1, result
        except sqlite3.Error as er_message:
            logger.error('Ups, error during select: {}'.format(er_message))
            return -1, result
        except KeyError as er_message:
            logger.error('Ups, error during select: {}'.format(er_message))
            return -1, result
        except yaml.parser.ParserError as er_message:
            logger.error('Ups, error during select: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute select')
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
        logger.info('end select in sqlite db = {}'.format(db_name))
        return 1, result

    else:
        logger.info('start select. Conn is sent to the function')
        cur = None
        result = None
        try:
            cur = conn.cursor()

            logger.debug('start execute select:\n{}'.format(script))
            cur.execute(script)
            result = cur.fetchall()
        except sqlite3.Error as er_message:
            logger.error('Ups, error during select: {}'.format(er_message))
            return -1, result
        else:
            logger.debug('end execute select')
        finally:
            if cur:
                cur.close()
        logger.info('end select. Conn is sent to the function')
        return 1, result


def sqlite_execute_with_commit(script, db_name, conn):
    if conn == 'none':
        logger.info('start execute script in sqlite db = {}'.format(db_name))
        cur = None
        conn = None
        try:
            with open(const.sqlite_conf_path, 'r') as file:
                db_conf = yaml.safe_load(file)

            conn = sqlite3.connect(db_conf['db'][db_name]['conn_string'])
            cur = conn.cursor()

            logger.debug('start execute script:\n{}'.format(script))
            cur.execute(script)
            conn.commit()
        except FileNotFoundError as er_message:
            logger.error('Ups, error during execute script: {}'.format(er_message))
            return -1
        except sqlite3.Error as er_message:
            logger.error('Ups, error during execute script: {}'.format(er_message))
            return -1
        except KeyError as er_message:
            logger.error('Ups, error during execute script: {}'.format(er_message))
            return -1
        except yaml.parser.ParserError as er_message:
            logger.error('Ups, error during execute script: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute script')
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
        logger.info('end execute script in sqlite db = {}'.format(db_name))
        return 1

    else:
        logger.info('start execute script. Conn is sent to the function')
        cur = None
        try:
            cur = conn.cursor()

            logger.debug('start execute script:\n{}'.format(script))
            cur.execute(script)
            conn.commit()
        except sqlite3.Error as er_message:
            logger.error('Ups, error during select: {}'.format(er_message))
            return -1
        else:
            logger.debug('end execute script')
        finally:
            if cur:
                cur.close()
        logger.info('end execute script. Conn is sent to the function')
        return 1
