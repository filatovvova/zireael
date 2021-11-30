import yaml
import logging.config


logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')

logger.debug('pampam')

with open('./configs/sqlite.yaml', 'r') as file:
    prime_service = yaml.safe_load(file)
print(prime_service)

for row in prime_service['db']:
    print(row)

