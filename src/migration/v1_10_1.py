import logging
from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import query

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


# identity service
@query
def identity_project_group_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'project_group', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'project_group', operations)


@query
def identity_role_binding_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'role_binding', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'role_binding', operations)


@query
def identity_project_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'project', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'project', operations)


@query
def identity_user_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'user', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'user', operations)


@query
def identity_service_account_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'service_account', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'service_account', operations)


@query
def identity_domain_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'domain', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'domain', operations)


@query
def identity_role_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'role', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'role', operations)


@query
def identity_provider_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'provider', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'provider', operations)


@query
def identity_policy_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'policy', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'policy', operations)


# monitoring service
@query
def monitoring_data_source_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('MONITORING', 'data_source', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('MONITORING', 'data_source', operations)


# statistic service
@query
def statistics_schedule_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('STATISTICS', 'schedule', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('STATISTICS', 'schedule', operations)


# secret service
@query
def secret_secret_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('SECRET', 'secret', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('SECRET', 'secret', operations)


@query
def secret_secret_group_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('SECRET', 'secret_group', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('SECRET', 'secret_group', operations)


# repository service
@query
def repository_schema_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('REPOSITORY', 'schema', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'schema', operations)


@query
def repository_plugin_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('REPOSITORY', 'plugin', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'plugin', operations)


@query
def repository_policy_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('REPOSITORY', 'policy', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'policy', operations)


@query
def plugin_supervisor_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('PLUGIN', 'supervisor', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('PLUGIN', 'supervisor', operations)


# config service
@query
def config_user_config_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('CONFIG', 'user_config', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('CONFIG', 'user_config', operations)


@query
def config_domain_config_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('CONFIG', 'domain_config', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('CONFIG', 'domain_config', operations)


# inventory service
@query
def inventory_server_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'server', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'server', operations)


@query
def inventory_resource_group_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'resource_group', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'resource_group', operations)


@query
def inventory_region_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'region', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'region', operations)


@query
def inventory_collector_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'collector', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'collector', operations)


@query
def inventory_cloud_service_type_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'cloud_service_type', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'cloud_service_type', operations)


@query
def inventory_cloud_service_tags_refactoring(mongo_client: MongoCustomClient):
    print('***** [EXECUTE] inventory_cloud_service_tags_refactoring] ******')
    cloud_services = mongo_client.find('INVENTORY', 'cloud_service', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": _change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'cloud_service', operations)


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
            new_dict[data[index]["key"]] = data[index]["value"]
    return new_dict


def main(file_path, debug):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, debug)

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

    # inventory service / 6 resources
    inventory_server_tags_refactoring(mongo_client)
    inventory_resource_group_tags_refactoring(mongo_client)
    inventory_region_tags_refactoring(mongo_client)
    inventory_collector_tags_refactoring(mongo_client)
    inventory_cloud_service_type_tags_refactoring(mongo_client)
    inventory_cloud_service_tags_refactoring(mongo_client)


if __name__ == '__main__':
    main(file_path=None, debug=False)
