import logging
import hashlib

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log
from pymongo import UpdateOne

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def inventory_collector_remove_plugin_info_secret_service_account_id_provider(
    mongo_client: MongoCustomClient,
):
    mongo_client.update_many(
        "INVENTORY",
        "collector",
        {},
        {
            "$unset": {
                "plugin_info.secret_id": "",
                "plugin_info.secret_group_id": "",
                "plugin_info.service_account_id": "",
                "plugin_info.provider": "",
            }
        },
    )


@print_log
def inventory_collector_remove_state_is_public(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "INVENTORY", "collector", {}, {"$unset": {"state": "", "is_public": ""}}
    )


@print_log
def inventory_collector_migrate_schedule(mongo_client: MongoCustomClient):
    schedules = mongo_client.find(
        "INVENTORY",
        "schedule",
        {},
        {
            "schedule_id": 1,
            "schedule": 1,
            "domain_id": 1,
            "collector_id": 1,
            "collector": 1,
        },
    )

    collector_ids = []
    operations = []
    count = 0
    for schedule in schedules:
        count += 1

        collector_id = schedule["collector_id"]
        if schedule.get("schedule", {}).get("hours", []):
            if collector_id not in collector_ids:
                collector_ids.append(schedule["collector_id"])
                operations.append(
                    UpdateOne(
                        {"_id": schedule["collector"]},
                        {
                            "$set": {
                                "schedule": {
                                    "state": "ENABLED",
                                    "hours": schedule["schedule"].get("hours", []),
                                }
                            }
                        },
                    )
                )

    mongo_client.bulk_write("INVENTORY", "collector", operations)


@print_log
def inventory_schedule_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection("INVENTORY", "schedule")


@print_log
def inventory_schedule_remove_index(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("INVENTORY", "schedule")


@print_log
def inventory_collector_remove_schedule(mongo_client: MongoCustomClient):
    mongo_client.update_many("INVENTORY", "collector", {}, {"$unset": {"schedule": ""}})


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v1.12.0")

    # inventory_collector_remove_schedule(mongo_client)

    inventory_collector_remove_plugin_info_secret_service_account_id_provider(
        mongo_client
    )
    inventory_collector_remove_state_is_public(mongo_client)

    inventory_collector_migrate_schedule(mongo_client)

    inventory_schedule_remove_index(mongo_client)
    # inventory_schedule_drop(mongo_client)
