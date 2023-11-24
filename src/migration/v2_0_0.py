import logging
from conf import DEFAULT_LOGGER
from pymongo import UpdateOne
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def cost_analysis_data_source_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('COST_ANALYSIS', 'data_source')


@print_log
def inventory_cloud_service_query_set_change_keys_to_data_keys(mongo_client: MongoCustomClient):
    cloud_service_query_sets = mongo_client.find('INVENTORY', 'cloud_service_query_set', {}, {'_id': 1, 'keys': 1})

    operations = []
    for cloud_service_query in cloud_service_query_sets:
        if 'keys' in cloud_service_query:
            operations.append(
                UpdateOne(
                    {'_id': cloud_service_query['_id']},
                    {"$rename": {'keys': 'data_keys'}}
                )
            )

    if operations:
        mongo_client.bulk_write('INVENTORY', 'cloud_service_query_set', operations)


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, 'v2.0.0')

    cost_analysis_data_source_drop_indexes(mongo_client)
    inventory_cloud_service_query_set_change_keys_to_data_keys(mongo_client)
