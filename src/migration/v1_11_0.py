import logging
import hashlib
from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def repository_services_remove_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('REPOSITORY', 'plugin')
    mongo_client.drop_indexes('REPOSITORY', 'policy')
    mongo_client.drop_indexes('REPOSITORY', 'schema')


@print_log
def monitoring_alert_number_remove_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('MONITORING', 'alert_number')


@print_log
def monitoring_alert_refactor_alert_number_by_domain_id(mongo_client: MongoCustomClient):
    domain_ids = mongo_client.distinct('MONITORING', 'alert', 'domain_id')

    records = []
    for domain_id in domain_ids:
        target_filter = {
            "domain_id": domain_id,
            "alert_number_str": {"$exists": False}
        }
        alerts = mongo_client.find('MONITORING', 'alert',
                                   target_filter, {"_id": 1, "created_at": 1}).sort('created_at', 1)

        if alerts:
            alert_number = 0
            operations = []
            for number, alert in enumerate(alerts, 1):
                operations.append(
                    UpdateOne({'_id': alert['_id']}, {"$set": {"alert_number": number,
                                                               "alert_number_str": str(number)}})
                )
                alert_number = number

            records.append({"domain_id": domain_id, "next": alert_number})
            mongo_client.bulk_write('MONITORING', 'alert', operations)

            _LOGGER.info(f"alert_number changed (domain_id: {domain_id} / number: {alert_number})")

    valid_records = [record for record in records if record['next'] != 0]
    if valid_records:
        mongo_client.insert_many('MONITORING', 'alert_number', records, is_new=True)
        _LOGGER.info(f"alert_number collection created (record count: {len(valid_records)})")


@print_log
def monitoring_escalation_policy_change_scope_from_global_to_domain(mongo_client: MongoCustomClient):
    mongo_client.update_many('MONITORING', 'escalation_policy', {"scope": {"$eq": "GLOBAL"}},
                             {"$set": {'scope': 'DOMAIN'}})


@print_log
def inventory_cloud_service_refactor_data_structure(mongo_client: MongoCustomClient):
    projection = {
        'provider': 1,
        'metadata': 1,
        'tags': 1,
        'collection_info': 1
    }
    target_filter = {'tags': {'$type': 'array'}}

    for cloud_services in mongo_client.find_by_pagination('INVENTORY', 'cloud_service', target_filter, projection,
                                                          show_progress=True):
        operations = []
        for cloud_service in cloud_services:
            provider = cloud_service.get('provider', 'custom')
            metadata = cloud_service.get('metadata', {})
            tags = cloud_service.get('tags', {})
            collection_info = cloud_service.get('collection_info', {})

            update_fields = {"$set": {}}

            if tags and isinstance(tags, list):
                new_tags = {}
                new_tag_keys = {}

                for tag in tags:
                    tag_key = str(tag['key'])
                    tag_value = str(tag.get('value', ''))
                    tag_provider = str(tag.get('provider', 'custom'))

                    hashed_key = string_to_hash(tag_key)

                    new_tags[tag_provider] = new_tags.get(tag_provider, {})
                    new_tags[tag_provider][hashed_key] = {'key': tag_key, 'value': tag_value}

                    new_tag_keys[tag_provider] = new_tag_keys.get(tag_provider, [])
                    new_tag_keys[tag_provider].append(tag_key)

                for provider, tag_keys in new_tag_keys.items():
                    new_tag_keys[provider] = list(set(tag_keys))

                update_fields['$set'].update({'tags': new_tags})
                update_fields['$set'].update({'tag_keys': new_tag_keys})

            elif isinstance(tags, list):
                update_fields['$set'].update({'tags': {}})

            if metadata and provider not in metadata:
                new_metadata = {}
                for plugin_id in metadata:
                    new_metadata = {provider: metadata[plugin_id]}

                update_fields['$set'].update({'metadata': new_metadata})

            if collection_info and isinstance(collection_info, dict):
                update_fields['$set'].update({'collection_info': []})

            if len(update_fields['$set'].keys()) > 0:
                operations.append(UpdateOne({'_id': cloud_service['_id']}, update_fields))

        mongo_client.bulk_write('INVENTORY', 'cloud_service', operations)


@print_log
def cost_analysis_data_source_rule_set_rule_type(mongo_client: MongoCustomClient):
    mongo_client.update_many('COST-ANALYSIS', 'data_source_rule', {"rule_type": {"$exists": False}},
                             {"$set": {'rule_type': 'MANAGED'}})


@print_log
def inventory_server_remove_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'server')


@print_log
def inventory_zone_remove_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'zone')


@print_log
def inventory_cloud_service_tag_remove_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'cloud_service_tag')


def string_to_hash(str_value: str) -> str:
    """MD5 hash of a String."""
    dhash = hashlib.md5(str_value.encode("utf-8"))
    return dhash.hexdigest()


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, 'v1.11.0')

    # remove index of repository services
    repository_services_remove_indexes(mongo_client)

    # refactor alert_number
    monitoring_alert_number_remove_collection(mongo_client)
    monitoring_alert_refactor_alert_number_by_domain_id(mongo_client)

    # change_scope in escalation_policy
    monitoring_escalation_policy_change_scope_from_global_to_domain(mongo_client)

    # change schema of cloud_service
    inventory_cloud_service_refactor_data_structure(mongo_client)

    # add rule_type in data_source_rule of cost-analysis
    cost_analysis_data_source_rule_set_rule_type(mongo_client)

    # remove unused collections
    inventory_cloud_service_tag_remove_collection(mongo_client)
    inventory_server_remove_collection(mongo_client)
    inventory_zone_remove_collection(mongo_client)
