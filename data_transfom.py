import logging.config
from configs import const
import yaml
from zireael import db_execute_with_commit
import sys
import argparse

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


# with open(const.data_transform_path, 'r') as file:
#     db_conf = yaml.safe_load(file)
#

# for steps in db_conf['jobs']:
#     for step in db_conf['jobs'][steps]:
#         with open(db_conf['jobs'][steps][step]['step_script'], 'r') as file:
#             script = file.read().format("DATE('now') - " + str(const.depth_of_calculations_in_days), "DATE('now')")
#         db_execute_with_commit(script=script, db_type='sqlite', db_name=const.sqlite_db_name)


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-sd', '--start_date', default=None)
    parser.add_argument('-ed', '--end_date', default=None)

    return parser


def start_job(job_name, db_type, start_date, end_date):
    logger.info('start execute job: {}'.format(job_name))
    try:
        with open(const.data_transform_path, 'r') as file:
            dt_conf = yaml.safe_load(file)
    except FileNotFoundError as er_message:
        logger.error('Ups, error during open yaml: {}'.format(er_message))
        return -1
    except KeyError as er_message:
        logger.error('Ups, error during open yaml: {}'.format(er_message))
        return -1
    else:
        logger.debug('yaml conf for job is open')

    stop_transform_if_error = dt_conf['jobs'][job_name]['end_if_error']
    for step in dt_conf['jobs'][job_name]['steps']:
        logger.info("start job's step: {}".format(dt_conf['jobs'][job_name]['steps'][step]['step_name']))
        error_flag = False
        try:
            with open(dt_conf['jobs'][job_name]['steps'][step]['step_script'], 'r') as file:
                script = file.read().format(start_date, end_date)
        except FileNotFoundError as er_message:
            logger.error('Ups, error during open transform script: {}'.format(er_message))
            if stop_transform_if_error:
                return -1
            else:
                error_flag = True
        except KeyError as er_message:
            logger.error('Ups, error during open transform script: {}'.format(er_message))
            if stop_transform_if_error:
                return -1
            else:
                error_flag = True
        else:
            logger.debug('transform script is open')

        if error_flag is False:
            resp = db_execute_with_commit(script=script, db_type=db_type, db_name=const.sqlite_db_name)
            if resp != 1 and stop_transform_if_error is True:
                logger.error('stop job "{}" with error'.format(job_name))
                return -1
        logger.info("end job's step: {}".format(dt_conf['jobs'][job_name]['steps'][step]['step_name']))

    return 1
    logger.info('end execute job: {}'.format(job_name))


if __name__ == '__main__':
    logger.info('start data transform')

    parser = create_parser()
    namespace = parser.parse_args(sys.argv[1:])

    logger.debug('start_date from command line: {}. end_date from command line: {}'.format(str(namespace.start_date),
                                                                                           str(namespace.end_date)))

    if namespace.start_date:
        start_date = str(namespace.start_date)
    else:
        start_date = "DATE('now', 'localtime') - " + str(const.depth_of_calculations_in_days)

    if namespace.end_date:
        end_date = str(namespace.end_date)
    else:
        end_date = "DATE('now', 'localtime')"
    logger.debug('result start_date: {}. result end_date: {}'.format(start_date, end_date))

    # block with start jobs for data transform
    resp = start_job('error_in_the_temperature_forecast_for_the_day', 'sqlite', start_date, end_date)
    print(resp)

    logger.info('end data transform')



