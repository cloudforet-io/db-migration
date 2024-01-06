import logging
from datetime import datetime

from pymongo import UpdateOne
from spaceone.core.utils import generate_id

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def event_rule_update_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "MONITORING",
        "event_rule",
        {},
        {"$rename": {"scope": "resource_group"}},
    )


@print_log
def alert_update_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "MONITORING",
        "alert",
        {},
        {
            "$unset": {
                "status_message": 1,
                "is_snoozed": 1,
                "snoozed_end_time": 1,
                "responders": 1,
                "project_dependencies": 1,
            }
        },
    )


@print_log
def drop_collections(mongo_client: MongoCustomClient):
    collections = ["alert_number", "maintenance_window"]
    for collection in collections:
        mongo_client.drop_collection("MONITORING", collection)


@print_log
def monitoring_escalation_policy_refactoring(
    mongo_client, domain_id_param, project_map, workspace_mode
):
    delete_escalation_policy_ids = []

    escalation_policy_infos = mongo_client.find(
        "MONITORING", "escalation_policy", {"domain_id": domain_id_param}, {}
    )

    for escalation_policy_info in escalation_policy_infos:
        if escalation_policy_info.get("workspace_id"):
            continue
        if not project_map[domain_id_param]:
            _LOGGER.error(
                f"Domain({domain_id_param}) has no project map data (domain_id: {domain_id_param})"
            )
            continue

        if escalation_policy_info.get("scope") == "DOMAIN":
            resource_group = "WORKSPACE"
            project_id = "*"
        else:
            resource_group = "PROJECT"
            project_id = escalation_policy_info.get("project_id")

        if not workspace_mode:
            workspace_id = list(
                project_map[escalation_policy_info["domain_id"]].values()
            )[0]
            set_params = {
                "$set": {
                    "resource_group": resource_group,
                    "workspace_id": workspace_id,
                    "project_id": project_id,
                },
                "$unset": {"scope": 1},
            }
            mongo_client.update_one(
                "MONITORING",
                "escalation_policy",
                {"_id": escalation_policy_info["_id"]},
                set_params,
            )
        else:
            workspace_infos = mongo_client.find(
                "IDENTITY",
                "workspace",
                {"domain_id": escalation_policy_info["domain_id"]},
                {},
            )
            for workspace_info in workspace_infos:
                create_escalation_policy_param = {
                    "name": escalation_policy_info["name"],
                    "is_default": escalation_policy_info["is_default"],
                    "rules": escalation_policy_info["rules"],
                    "repeat_count": escalation_policy_info["repeat_count"],
                    "finish_condition": escalation_policy_info["finish_condition"],
                    "tags": escalation_policy_info["tags"],
                    "resource_group": resource_group,
                    "project_id": "*",
                    "domain_id": escalation_policy_info["domain_id"],
                    "created_at": datetime.utcnow(),
                }
                workspace_id = workspace_info["workspace_id"]
                new_escalation_policy_id = generate_id("ep")
                create_escalation_policy_param.update(
                    {
                        "workspace_id": workspace_id,
                        "escalation_policy_id": new_escalation_policy_id,
                    }
                )
                mongo_client.insert_one(
                    "MONITORING",
                    "escalation_policy",
                    create_escalation_policy_param,
                    is_new=True,
                )

                new_workspace_info = mongo_client.find_one(
                    "MONITORING",
                    "escalation_policy",
                    {"escalation_policy_id": new_escalation_policy_id},
                    {},
                )

                update_project_alert_config_params = {
                    "$set": {
                        "escalation_policy": new_workspace_info["_id"],
                        "escalation_policy_id": new_workspace_info[
                            "escalation_policy_id"
                        ],
                    }
                }
                mongo_client.update_many(
                    "MONITORING",
                    "project_alert_config",
                    {
                        "domain_id": escalation_policy_info["domain_id"],
                        "escalation_policy_id": escalation_policy_info[
                            "escalation_policy_id"
                        ],
                        "workspace_id": workspace_id,
                    },
                    update_project_alert_config_params,
                )
                update_alert_params = {
                    "$set": {
                        "escalation_policy_id": new_workspace_info[
                            "escalation_policy_id"
                        ]
                    }
                }
                mongo_client.update_many(
                    "MONITORING",
                    "alert",
                    {
                        "domain_id": escalation_policy_info["domain_id"],
                        "escalation_policy_id": escalation_policy_info[
                            "escalation_policy_id"
                        ],
                        "workspace_id": workspace_id,
                    },
                    update_alert_params,
                )

            delete_escalation_policy_ids.append(escalation_policy_info["_id"])

    mongo_client.delete_many(
        "MONITORING",
        "escalation_policy",
        {"_id": {"$in": delete_escalation_policy_ids}},
    )


@print_log
def monitoring_project_alert_config_update_fields(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    operations = []

    project_alert_config_infos = mongo_client.find(
        "MONITORING",
        "project_alert_config",
        {"domain_id": domain_id},
        {},
    )

    for project_alert_config_info in project_alert_config_infos:
        if project_alert_config_info.get("workspace_id"):
            continue

        workspace_id = project_map[project_alert_config_info["domain_id"]].get(
            project_alert_config_info["project_id"]
        )
        operations.append(
            UpdateOne(
                {"_id": project_alert_config_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("MONITORING", "project_alert_config", operations)


@print_log
def monitoring_event_rule_update_fields(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    operations = []

    event_rule_infos = mongo_client.find(
        "MONITORING",
        "event_rule",
        {"domain_id": domain_id},
        {},
    )

    for event_rule_info in event_rule_infos:
        if event_rule_info.get("workspace_id"):
            continue

        workspace_id = project_map[event_rule_info["domain_id"]].get(
            event_rule_info["project_id"]
        )
        operations.append(
            UpdateOne(
                {"_id": event_rule_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("MONITORING", "event_rule", operations)


@print_log
def monitoring_webhook_update_fields(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    operations = []

    webhook_infos = mongo_client.find(
        "MONITORING",
        "webhook",
        {"domain_id": domain_id},
        {},
    )

    for webhook_info in webhook_infos:
        if webhook_info.get("workspace_id"):
            continue

        workspace_id = project_map[webhook_info["domain_id"]].get(
            webhook_info["project_id"]
        )
        operations.append(
            UpdateOne(
                {"_id": webhook_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("MONITORING", "webhook", operations)


@print_log
def monitoring_alert_update_fields(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    operations = []

    alert_infos = mongo_client.find(
        "MONITORING",
        "alert",
        {"domain_id": domain_id},
        {},
    )

    for alert_info in alert_infos:
        if alert_info.get("workspace_id"):
            continue

        workspace_id = project_map[alert_info["domain_id"]].get(
            alert_info["project_id"]
        )
        operations.append(
            UpdateOne(
                {"_id": alert_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("MONITORING", "alert", operations)


@print_log
def monitoring_event_update_fields(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    operations = []

    event_infos = mongo_client.find(
        "MONITORING",
        "event",
        {"domain_id": domain_id},
        {},
    )

    for event_info in event_infos:
        if event_info.get("workspace_id"):
            continue

        workspace_id = project_map[event_info["domain_id"]].get(
            event_info["project_id"]
        )
        operations.append(
            UpdateOne(
                {"_id": event_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("MONITORING", "event", operations)


@print_log
def monitoring_note_update_fields(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    operations = []

    note_infos = mongo_client.find(
        "MONITORING",
        "note",
        {"domain_id": domain_id},
        {},
    )

    for note_info in note_infos:
        if note_info.get("workspace_id"):
            continue

        workspace_id = project_map[note_info["domain_id"]].get(note_info["project_id"])
        operations.append(
            UpdateOne(
                {"_id": note_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("MONITORING", "note", operations)


@print_log
def monitoring_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("MONITORING", "*")


def main(mongo_client, domain_id, project_map, workspace_mode):
    monitoring_drop_indexes(mongo_client)
    monitoring_project_alert_config_update_fields(mongo_client, domain_id, project_map)
    monitoring_event_rule_update_fields(mongo_client, domain_id, project_map)
    monitoring_webhook_update_fields(mongo_client, domain_id, project_map)
    monitoring_alert_update_fields(mongo_client, domain_id, project_map)
    monitoring_event_update_fields(mongo_client, domain_id, project_map)
    monitoring_note_update_fields(mongo_client, domain_id, project_map)
    monitoring_escalation_policy_refactoring(
        mongo_client, domain_id, project_map, workspace_mode
    )
