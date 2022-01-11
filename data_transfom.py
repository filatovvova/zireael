import logging.config
from configs import const
from zireael import db_execute_with_commit
import sys
import argparse
from work_with_files import get_file_data, get_yaml_data

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-sd', '--start_date', default=None)
    parser.add_argument('-ed', '--end_date', default=None)

    return parser


def start_job(job_name, r_start_date, r_end_date):
    logger.info('start execute job: {}'.format(job_name))
    dt_conf = get_yaml_data(const.data_transform_path)
    if dt_conf == -1:
        return -1

    if r_start_date:
        start_date = r_start_date
    else:
        start_date = dt_conf['jobs'][job_name]['date_now'] + " - " + str(const.depth_of_calculations_in_days)

    if r_end_date:
        end_date = r_end_date
    else:
        end_date = dt_conf['jobs'][job_name]['date_now']
    logger.debug('result start_date: {}. result end_date: {}'.format(start_date, end_date))

    stop_transform_if_error = dt_conf['jobs'][job_name]['end_if_error']
    db_type = dt_conf['jobs'][job_name]['db_type']
    for step in dt_conf['jobs'][job_name]['steps']:
        logger.info("start job's step: {}".format(dt_conf['jobs'][job_name]['steps'][step]['step_name']))
        error_flag = False
        script = get_file_data(dt_conf['jobs'][job_name]['steps'][step]['step_script'])
        if stop_transform_if_error and script == -1:
            logger.error('transform process was stopped for job: {}'.format(job_name))
            return -1
        elif script == -1:
            logger.error('step of transform was skipped due to the mistake. the data transformation process will '
                         'continue with the next step')
            error_flag = True
        else:
            script = script.format(start_date, end_date)

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
    # block with start jobs for data transform
    resp = start_job('error_in_the_temperature_forecast_for_the_day_sqlite', namespace.start_date, namespace.end_date)
    print(resp)
    resp = start_job('error_in_the_temperature_forecast_for_the_day_mysql', namespace.start_date, namespace.end_date)
    print(resp)

    logger.info('end data transform')



