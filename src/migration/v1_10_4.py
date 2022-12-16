import logging
from conf import DEFAULT_LOGGER
from pymongo import UpdateOne, DeleteOne
from lib import MongoCustomClient
from lib.util import query, check_time

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@query
@check_time
def inventory_record_delete_project_id(mongo_client: MongoCustomClient):
    item_count = 0
    for items in mongo_client.find_by_pagination('INVENTORY', 'record', {}, {'_id': 1}):
        operations = []
        for item in items:
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$unset": {"project_id": ""}})
            )
            item_count += 1

        mongo_client.bulk_write('INVENTORY', 'record', operations)
        _LOGGER.debug(f'Total Count : {item_count}')


@query
@check_time
def inventory_cloud_service_tag_delete_project_id(mongo_client: MongoCustomClient):
    item_count = 0
    for items in mongo_client.find_by_pagination('INVENTORY', 'cloud_service_tag', {}, {'_id': 1}):
        operations = []
        for item in items:
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$unset": {"project_id": ""}})
            )
            item_count += 1

        mongo_client.bulk_write('INVENTORY', 'cloud_service_tag', operations)
        _LOGGER.debug(f'Total Count : {item_count}')


@query
def inventory_cloud_service_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('INVENTORY', 'cloud_service')


@query
def inventory_cloud_service_tag_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('INVENTORY', 'cloud_service_tag')


@query
def inventory_collection_state_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('INVENTORY', 'collection_state')


@query
def inventory_record_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('INVENTORY', 'record')


@query
def inventory_cloud_service_type_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('INVENTORY', 'cloud_service_type')


@query
def inventory_region_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('INVENTORY', 'region')


def main(file_path, debug):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, debug)

    inventory_record_delete_project_id(mongo_client)
    inventory_cloud_service_tag_delete_project_id(mongo_client)

    inventory_cloud_service_drop_indexes(mongo_client)
    inventory_cloud_service_tag_drop_indexes(mongo_client)
    inventory_collection_state_drop_indexes(mongo_client)
    inventory_record_drop_indexes(mongo_client)
    inventory_cloud_service_type_drop_indexes(mongo_client)
    inventory_region_drop_indexes(mongo_client)
