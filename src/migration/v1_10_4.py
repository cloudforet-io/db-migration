import logging
from conf import DEFAULT_LOGGER
from pymongo import UpdateOne, DeleteOne
from lib import MongoCustomClient
from lib.util import query

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@query
def inventory_cloud_service_tag_delete_project_id(mongo_client: MongoCustomClient):
    items = mongo_client.find('INVENTORY', 'cloud_service_tag', {}, {'project_id': 1})

    operations = []
    for item in items:
        operations.append(
            UpdateOne({'_id': item['_id']}, {"$unset": {"project_id": ""}})
        )
    mongo_client.bulk_write('INVENTORY', 'cloud_service_tag', operations)


def main(file_path, debug):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, debug)

    inventory_cloud_service_tag_delete_project_id(mongo_client)
