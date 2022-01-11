import logging.config
import yaml

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


def get_yaml_data(yaml_file_path):
    logger.debug('start get yaml data from path: {}'.format(yaml_file_path))
    try:
        with open(yaml_file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
    except FileNotFoundError as er_message:
        logger.error('Ups, error during open yaml: {}'.format(er_message))
        return -1
    except KeyError as er_message:
        logger.error('Ups, error during open yaml: {}'.format(er_message))
        return -1
    except yaml.parser.ParserError as er_message:
        logger.error('Ups, error during open yaml: {}'.format(er_message))
        return -1
    else:
        logger.debug('yaml data got from path: {}'.format(yaml_file_path))
        return yaml_data


def get_file_data(file_path):
    logger.debug('start get file data from path: {}'.format(file_path))
    try:
        with open(file_path, 'r') as file:
            file_data = file.read()
    except FileNotFoundError as er_message:
        logger.error('Ups, error during open file: {}'.format(er_message))
        return -1
    except KeyError as er_message:
        logger.error('Ups, error during open file: {}'.format(er_message))
        return -1
    else:
        logger.debug('file data got from path: {}'.format(file_path))
        return file_data
