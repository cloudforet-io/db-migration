import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def cost_analysis_data_source_change_state_to_schedule(mongo_client: MongoCustomClient):
    enabled_update = {"$set": {"schedule": {"state": "ENABLED", "hour": 16}}}
    disabled_update = {"$set": {"schedule": {"state": "DISABLED"}}}

    mongo_client.update_many(
        "COST_ANALYSIS","data_source",{"state": "ENABLED"},
        enabled_update,
    )
    mongo_client.update_many("COST_ANALYSIS","data_source",{"state": "DISABLED"},
        disabled_update
    )


@print_log
def cost_analysis_data_source_delete_state_filed(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "COST_ANALYSIS","data_source",{},
        {"$unset": {"state": ""}}
    )


@print_log
def cost_analysis_data_source_change_schedule_to_state(mongo_client: MongoCustomClient):
    enabled_update = {"$set": {"state": "ENABLED"}}
    disabled_update = {"$set": {"state": "DISABLED"}}

    mongo_client.update_many(
        "COST_ANALYSIS","data_source",{"schedule.state": "ENABLED"},
        enabled_update,
    )
    mongo_client.update_many("COST_ANALYSIS","data_source",{"schedule.state": "DISABLED"},
        disabled_update
    )


@print_log
def cost_analysis_data_source_delete_schedule_filed(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "COST_ANALYSIS","data_source",{},
        {"$unset": {"schedule": ""}}
    )


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.4")

    # Cost Analysis Service
    cost_analysis_data_source_change_state_to_schedule(mongo_client)
    cost_analysis_data_source_delete_state_filed(mongo_client)

    #RollBack
    # cost_analysis_data_source_change_schedule_to_state(mongo_client)
    # cost_analysis_data_source_delete_schedule_filed(mongo_client)