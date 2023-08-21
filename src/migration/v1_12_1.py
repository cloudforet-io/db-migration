import logging
from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log


_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def inventory_job_task_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'job')


@print_log
def inventory_job_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'job_task')



def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, 'v1.12.1')
    inventory_job_task_drop(mongo_client)
    inventory_job_drop(mongo_client)
