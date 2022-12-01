import logging
from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import query

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@query
def portal_repository_schema_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('PORTAL-REPOSITORY', 'schema', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('PORTAL-REPOSITORY', 'schema', operations)


# Marketplace
@query
def portal_repository_plugin_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('PORTAL-REPOSITORY', 'plugin', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('PORTAL-REPOSITORY', 'plugin', operations)


@query
def portal_repository_policy_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('PORTAL-REPOSITORY', 'policy', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('PORTAL-REPOSITORY', 'policy', operations)


def _change_tags(data):
    """ convert tags type ( list of dict -> dict )
    [AS-IS]
            1) general type >>> {"tags": [{key: 'type', value: 'test'}, {key: 'user', value: 'developer'}]}
            2) empty list   >>> {"tags": []}
            3) dict type    >>> {"tags":{"type":"test"}}
    [TO-BE]
            1) general type >>>  {"tags": {'type':'test', 'user':'developer'}}
            2) empty list   >>>  {"tags":{}}
            3) dict type    >>> {"tags":{"type":"test"}}
    """
    new_dict = {}
    if not len(data):
        return new_dict
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        for index in range(len(data)):
            new_dict[data[index]["key"]] = data[index].get("value", "")
    return new_dict


def main(file_path, debug):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, debug)

    # remote repository service / 3 resources
    portal_repository_schema_tags_refactoring(mongo_client)
    portal_repository_plugin_tags_refactoring(mongo_client)
    portal_repository_policy_tags_refactoring(mongo_client)
