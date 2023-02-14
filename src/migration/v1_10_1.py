import logging
from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


# identity service
@print_log
def identity_project_group_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'project_group', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'project_group', operations)


@print_log
def identity_role_binding_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'role_binding', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'role_binding', operations)


@print_log
def identity_project_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'project', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'project', operations)


@print_log
def identity_user_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'user', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'user', operations)


@print_log
def identity_service_account_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'service_account', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'service_account', operations)


@print_log
def identity_domain_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'domain', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'domain', operations)


@print_log
def identity_role_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'role', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'role', operations)


@print_log
def identity_provider_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'provider', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'provider', operations)


@print_log
def identity_policy_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('IDENTITY', 'policy', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'policy', operations)


# monitoring service
@print_log
def monitoring_data_source_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('MONITORING', 'data_source', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('MONITORING', 'data_source', operations)


# statistic service
@print_log
def statistics_schedule_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('STATISTICS', 'schedule', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('STATISTICS', 'schedule', operations)


# secret service
@print_log
def secret_secret_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('SECRET', 'secret', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('SECRET', 'secret', operations)


@print_log
def secret_secret_group_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('SECRET', 'secret_group', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('SECRET', 'secret_group', operations)


# repository service
@print_log
def repository_schema_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('REPOSITORY', 'schema', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'schema', operations)


@print_log
def repository_plugin_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('REPOSITORY', 'plugin', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'plugin', operations)


@print_log
def repository_policy_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('REPOSITORY', 'policy', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'policy', operations)


@print_log
def plugin_supervisor_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('PLUGIN', 'supervisor', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('PLUGIN', 'supervisor', operations)


# config service
@print_log
def config_user_config_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('CONFIG', 'user_config', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('CONFIG', 'user_config', operations)


@print_log
def config_domain_config_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('CONFIG', 'domain_config', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('CONFIG', 'domain_config', operations)


# inventory service
@print_log
def inventory_resource_group_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('INVENTORY', 'resource_group', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'resource_group', operations)


@print_log
def inventory_region_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('INVENTORY', 'region', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'region', operations)


@print_log
def inventory_collector_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('INVENTORY', 'collector', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'collector', operations)


@print_log
def inventory_cloud_service_type_tags_refactoring(mongo_client: MongoCustomClient):
    items = mongo_client.find('INVENTORY', 'cloud_service_type', {}, {'tags': 1})

    operations = []
    for item in items:
        if isinstance(item['tags'], list):
            operations.append(
                UpdateOne({'_id': item['_id']}, {"$set": {"tags": _change_tags(item['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'cloud_service_type', operations)


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


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, 'v1.10.1')

    # execute migration functions
    # identity service / 9 resources
    identity_project_group_tags_refactoring(mongo_client)
    identity_role_binding_tags_refactoring(mongo_client)
    identity_project_tags_refactoring(mongo_client)
    identity_user_tags_refactoring(mongo_client)
    identity_service_account_tags_refactoring(mongo_client)
    identity_domain_tags_refactoring(mongo_client)
    identity_role_tags_refactoring(mongo_client)
    identity_provider_tags_refactoring(mongo_client)
    identity_policy_tags_refactoring(mongo_client)

    # monitoring service / 1 resource
    monitoring_data_source_tags_refactoring(mongo_client)

    # statistics service / 1 resource
    statistics_schedule_tags_refactoring(mongo_client)

    # secret service / 2 resources
    secret_secret_tags_refactoring(mongo_client)
    secret_secret_group_tags_refactoring(mongo_client)

    # repository service / 3 resources
    repository_schema_tags_refactoring(mongo_client)
    repository_plugin_tags_refactoring(mongo_client)
    repository_policy_tags_refactoring(mongo_client)

    # plugin service / 1 resource
    plugin_supervisor_tags_refactoring(mongo_client)

    # config service / 2 resources
    config_user_config_tags_refactoring(mongo_client)
    config_domain_config_tags_refactoring(mongo_client)

    # inventory service / 4 resources
    inventory_resource_group_tags_refactoring(mongo_client)
    inventory_region_tags_refactoring(mongo_client)
    inventory_collector_tags_refactoring(mongo_client)
    inventory_cloud_service_type_tags_refactoring(mongo_client)
