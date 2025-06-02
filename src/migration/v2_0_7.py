import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

@print_log
def cost_report_config_migration(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report_config",
        {"scope": {"$exists": False}},
        {"$set": {"scope": "WORKSPACE"}}
    )

    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report_config",
        {"adjustment_options": {"$exists": False}},
        {"$set": {"adjustment_options": {"enabled": False}}}
    )

@print_log
def cost_report_migration(mongo_client: MongoCustomClient):
    cursor = mongo_client.find(
        "COST_ANALYSIS", "cost_report",
        {"name": {"$exists": False}}
    )
    for doc in cursor:
        name_value = doc.get("workspace_name", "")
        mongo_client.update_one(
            "COST_ANALYSIS", "cost_report",
            {"_id": doc["_id"]},
            {"$set": {"name": name_value}}
        )

    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report",
        {"workspace_name": {"$exists": True}},
        {"$unset": {"workspace_name": ""}}
    )
    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report",
        {"status": "SUCCESS", "is_adjusted": {"$exists": False}},
        {"$set": {"status": "DONE"}}
    )
    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report",
        {"is_adjusted": {"$exists": False}},
        {"$set": {"is_adjusted": False}}
    )
    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report",
        {"service_account_id": {"$exists": False}},
        {"$set": {"service_account_id": None}}
    )
    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report",
        {"project_id": {"$exists": False}},
        {"$set": {"project_id": None}}
    )

@print_log
def cost_report_data_migration(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report_data",
        {"is_adjusted": {"$exists": False}},
        {"$set": {"is_adjusted": False}}
    )
    mongo_client.update_many(
        "COST_ANALYSIS", "cost_report_data",
        {"report_adjustment_policy_id": {"$exists": False}},
        {"$set": {"report_adjustment_policy_id": None}}
    )

def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.7")

    cost_report_config_migration(mongo_client)
    cost_report_migration(mongo_client)
    cost_report_data_migration(mongo_client)