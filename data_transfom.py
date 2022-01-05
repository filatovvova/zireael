import logging.config
from configs import const
import yaml
from zireael import db_execute_with_commit

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')

with open(const.data_transform_path, 'r') as file:
    db_conf = yaml.safe_load(file)

for steps in db_conf['jobs']:
    for step in db_conf['jobs'][steps]:
        print(db_conf['jobs'][steps][step])
        with open(db_conf['jobs'][steps][step]['step_script'], 'r') as file:
            print(file.read())

