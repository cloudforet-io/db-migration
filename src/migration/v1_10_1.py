from pymongo import UpdateOne

from lib import MongoCustomClient


def change_tags(data):
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


# execute function ({service}_{resource}_{purpose})

# identity service
def identity_project_group_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'project_group', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'project_group', operations)


def identity_role_binding_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'role_binding', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'role_binding', operations)


def identity_project_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'project', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'project', operations)


def identity_user_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'user', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'user', operations)


def identity_service_account_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'service_account', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'service_account', operations)


def identity_domain_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'domain', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'domain', operations)


def identity_role_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'role', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'role', operations)


def identity_provider_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'provider', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'provider', operations)


def identity_policy_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('IDENTITY', 'policy', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('IDENTITY', 'policy', operations)


# monitoring service
def monitoring_data_source_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('MONITORING', 'data_source', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('MONITORING', 'data_source', operations)


# statistic service
def statistics_schedule_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('STATISTICS', 'schedule', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('STATISTICS', 'schedule', operations)


# secret service
def secret_secret_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('SECRET', 'secret', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('SECRET', 'secret', operations)


def secret_secret_group_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('SECRET', 'secret_group', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('SECRET', 'secret_group', operations)


# repository service
def repository_schema_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('REPOSITORY', 'schema', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'schema', operations)


def repository_plugin_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('REPOSITORY', 'plugin', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'plugin', operations)


def repository_policy_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('REPOSITORY', 'policy', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('REPOSITORY', 'policy', operations)


def plugin_supervisor_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('PLUGIN', 'supervisor', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('PLUGIN', 'supervisor', operations)


# config service
def config_user_config_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('CONFIG', 'user_config', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('CONFIG', 'user_config', operations)


def config_domain_config_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('CONFIG', 'domain_config', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('CONFIG', 'domain_config', operations)


# inventory service
def inventory_server_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'server', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'server', operations)


def inventory_resource_group_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'resource_group', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'resource_group', operations)


def inventory_region_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'region', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'region', operations)


def inventory_collector_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'collector', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'collector', operations)


def inventory_cloud_service_type_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'cloud_service_type', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'cloud_service_type', operations)


def inventory_cloud_service_tags_refactoring(mongo_client: MongoCustomClient):
    cloud_services = mongo_client.find('INVENTORY', 'cloud_service', {})

    operations = []
    for cloud_service in cloud_services:
        if isinstance(cloud_service['tags'], list):
            operations.append(
                UpdateOne({'_id': cloud_service['_id']}, {"$set": {"tags": change_tags(cloud_service['tags'])}})
            )

    mongo_client.bulk_write('INVENTORY', 'cloud_service', operations)


def main(connection_uri, file_path, debug):
    mongo_client: MongoCustomClient = MongoCustomClient(connection_uri, file_path, debug)

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
    main(connection_uri='localhost:27017', file_path=None, debug=False)
