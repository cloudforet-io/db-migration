import logging

from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


def drop_collections(mongo_client: MongoCustomClient):
    collections = [
        "region",
        "cloud_service_type",
        "job",
        "job_task",
        "resource_group",
        "cloud_service_stats_query_history",
    ]

    for collection in collections:
        mongo_client.drop_collection("INVENTORY", collection)


def cloud_service_report_update_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "INVENTORY",
        "cloud_service_report",
        {},
        {
            "$set": {"resource_group": "DOMAIN", "workspace_id": "*"},
        },
    )


def collector_update_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "INVENTORY",
        "collector",
        {},
        {
            "$set": {"resource_group": "DOMAIN", "workspace_id": "*"},
        },
    )


def collector_rule_update_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "INVENTORY",
        "collector_rule",
        {},
        {
            "$set": {"resource_group": "DOMAIN", "workspace_id": "*"},
        },
    )


def inventory_collector_drop_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "INVENTORY",
        "collector",
        {},
        {
            "$unset": {"priority": 1},
        },
    )


@print_log
def inventory_cloud_service_refactoring(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    operations = []

    inventory_cloud_services_info = mongo_client.find(
        "INVENTORY", "cloud_service", {"domain_id": domain_id}, {}
    )

    for inventory_cloud_service_info in inventory_cloud_services_info:
        if inventory_cloud_service_info.get("workspace_id"):
            continue

        workspace_id = project_map[inventory_cloud_service_info["domain_id"]].get(
            inventory_cloud_service_info["project_id"]
        )
        operations.append(
            UpdateOne(
                {"_id": inventory_cloud_service_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("INVENTORY", "cloud_service", operations)


@print_log
def inventory_note_refactoring(mongo_client: MongoCustomClient, domain_id, project_map):
    operations = []

    inventory_notes_info = mongo_client.find(
        "INVENTORY", "note", {"domain_id": domain_id}, {}
    )

    for inventory_note_info in inventory_notes_info:
        if inventory_note_info.get("workspace_id"):
            continue

        workspace_id = project_map[inventory_note_info["domain_id"]].get(
            inventory_note_info["project_id"]
        )
        operations.append(
            UpdateOne(
                {"_id": inventory_note_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("INVENTORY", "note", operations)


def main(mongo_client: MongoCustomClient, domain_id, project_map):
    inventory_cloud_service_refactoring(mongo_client, domain_id, project_map)
    inventory_note_refactoring(mongo_client, domain_id, project_map)
    inventory_collector_drop_fields(mongo_client)
