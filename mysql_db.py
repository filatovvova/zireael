import mysql.connector
from mysql.connector import errorcode
import logging.config
import yaml
from configs import const

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


def mysql_bulk_insert(table_name, data_set, fields_name_pattern, values_pattern, db_name, conn):
    script = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, fields_name_pattern, values_pattern)
    if conn == 'none':
        logger.info('start bulk insert into table {} in mysql db = {}'.format(table_name, db_name))
        cur = None
        conn = None
        try:
            with open(const.mysql_conf_path, 'r') as file:
                db_conf = yaml.safe_load(file)

            # not a good practice
            with open(const.mysql_file, 'r') as file:
                psw = file.read()

            conn = mysql.connector.connect(user=db_conf['db'][db_name]['user'],
                                           password=psw,
                                           host=db_conf['db'][db_name]['host'],
                                           database=db_conf['db'][db_name]['database'],
                                           raise_on_warnings=db_conf['db'][db_name]['raise_on_warnings']
                                           )
            cur = conn.cursor()

            logger.debug('start execute bulk insert:\n{}'.format(script))
            cur.executemany(script, data_set)
            conn.commit()
        except FileNotFoundError as er_message:
            logger.error('Ups, error during bulk insert: {}'.format(er_message))
            return -1
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Ups, something is wrong with your user name or password: {}".format(err.msg))
                return -1
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Ups, database does not exist: {}".format(err.msg))
                return -1
            else:
                logger.error('Ups, error during execute script: {}. err number = {}'.format(err.msg, err.errno))
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
        logger.info('end bulk insert into table {} in mysql db = {}'.format(table_name, db_name))
        return 1

    else:
        logger.info('start bulk insert into table {} mysql db. Conn is sent to the function'.format(table_name))
        cur = None
        try:
            cur = conn.cursor()

            logger.debug('start execute bulk insert:\n{}'.format(script))
            cur.executemany(script, data_set)
            conn.commit()
        except mysql.connector.Error as err:
            logger.error('Ups, error during execute bulk insert: {}. err number = {}'.format(err.msg, err.errno))
            return -1
        else:
            logger.debug('end execute bulk insert')
        finally:
            if cur:
                cur.close()
        logger.info('end bulk insert into table {} mysql db. Conn is sent to the function'.format(table_name))
        return 1


def mysql_execute_with_commit(script, db_name, conn):
    if conn == 'none':
        logger.info('start execute script in mysql db = {}'.format(db_name))
        cur = None
        conn = None
        try:
            with open(const.mysql_conf_path, 'r') as file:
                db_conf = yaml.safe_load(file)

            # not a good practice
            with open(const.mysql_file, 'r') as file:
                psw = file.read()

            conn = mysql.connector.connect(user=db_conf['db'][db_name]['user'],
                                           password=psw,
                                           host=db_conf['db'][db_name]['host'],
                                           database=db_conf['db'][db_name]['database'],
                                           raise_on_warnings=db_conf['db'][db_name]['raise_on_warnings']
                                           )
            cur = conn.cursor()

            logger.debug('start execute script:\n{}'.format(script))
            cur.execute(script)
            conn.commit()
        except FileNotFoundError as er_message:
            logger.error('Ups, error during execute script: {}'.format(er_message))
            return -1
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Ups, something is wrong with your user name or password: {}".format(err.msg))
                return -1
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Ups, database does not exist: {}".format(err.msg))
                return -1
            else:
                logger.error('Ups, error during execute script: {}. err number = {}'.format(err.msg, err.errno))
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
        logger.info('end execute script in mysql db = {}'.format(db_name))
        return 1

    else:
        logger.info('start execute script. Conn is sent to the function')
        cur = None
        try:
            cur = conn.cursor()

            logger.debug('start execute script:\n{}'.format(script))
            cur.execute(script)
            conn.commit()
        except mysql.connector.Error as err:
            logger.error('Ups, error during execute script: {}. err number = {}'.format(err.msg, err.errno))
            return -1
        else:
            logger.debug('end execute script')
        finally:
            if cur:
                cur.close()
        logger.info('end execute script. Conn is sent to the function')
        return 1
