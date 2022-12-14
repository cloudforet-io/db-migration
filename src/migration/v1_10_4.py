import logging
from conf import DEFAULT_LOGGER
from pymongo import UpdateOne, DeleteOne
from lib import MongoCustomClient
from lib.util import query

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@query
def inventory_record_delete_project_id(mongo_client: MongoCustomClient):
    domains = mongo_client.find('IDENTITY', 'domain', {}, {'domain_id': 1})
    for domain in domains:
        items = mongo_client.find('INVENTORY', 'record', {'domain_id': domain['domain_id']}, {})

        operations = []
        for item in items:
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$unset": {"project_id": ""}})
            )
        mongo_client.bulk_write('INVENTORY', 'record', operations)


@query
def inventory_cloud_service_tag_delete_project_id(mongo_client: MongoCustomClient):
    domains = mongo_client.find('IDENTITY', 'domain', {}, {'domain_id': 1})
    for domain in domains:
        items = mongo_client.find('INVENTORY', 'cloud_service_tag', {'domain_id': domain['domain_id']}, {})

        operations = []
        for item in items:
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$unset": {"project_id": ""}})
            )
        mongo_client.bulk_write('INVENTORY', 'cloud_service_tag', operations)


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
