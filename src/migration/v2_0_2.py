import logging

from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient

from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

# def list_multi_domains(mongo_client: MongoCustomClient):
#     domain_cursor = mongo_client.find(
#         "IDENTITY",
#         "domain",
#         {"tags.workspace_mode": "multi"},
#         {"_id": 0, "domain_id": 1},
#     )
#
#     return [domain["domain_id"] for domain in domain_cursor]


CHECK_DOMAINS = ["domain-7c1dd832e097", "domain-42a0592c9e25", "domain-b5f42dfe6b19"]
WORKSPACE_MAP = {}


def set_workspace_map(mongo_client: MongoCustomClient):
    for domain_id in CHECK_DOMAINS:
        workspace_names = _list_diff_workspace_names(mongo_client, domain_id)
        for workspace_name in workspace_names:
            workspace_info = mongo_client.find_one(
                "IDENTITY-TO-BE",
                "workspace",
                {"domain_id": domain_id, "name": workspace_name},
                {},
            )

            project_group = mongo_client.find_one(
                "IDENTITY-TO-BE",
                "project_group",
                {"domain_id": domain_id, "name": workspace_name},
                {},
            )
            before_workspace_id = workspace_info["workspace_id"]
            after_workspace_id = project_group["workspace_id"]

            if domain_id not in WORKSPACE_MAP:
                WORKSPACE_MAP[domain_id] = {before_workspace_id: after_workspace_id}
            else:
                WORKSPACE_MAP[domain_id].update(
                    {before_workspace_id: after_workspace_id}
                )


@print_log
def change_workspace_id(
    mongo_client: MongoCustomClient, domain_id, service, collection, pagination=False
):
    for before_workspace_id, after_workspace_id in WORKSPACE_MAP[domain_id].items():
        if pagination:
            for collection_cursor in mongo_client.find_by_pagination(
                service,
                collection,
                {
                    "domain_id": domain_id,
                    "workspace_id": before_workspace_id,
                },
                {"_id": 1, "workspace_id": 1},
                show_progress=True,
            ):
                operations = []
                for document in collection_cursor:
                    operations.append(
                        UpdateOne(
                            {"_id": document["_id"]},
                            {"$set": {"workspace_id": after_workspace_id}},
                        )
                    )

                mongo_client.bulk_write(service, collection, operations)

        else:
            mongo_client.update_many(
                service,
                collection,
                {"domain_id": domain_id, "workspace_id": before_workspace_id},
                {"$set": {"workspace_id": after_workspace_id}},
            )


@print_log
def delete_resources(mongo_client: MongoCustomClient, domain_id, service, collection):
    for before_workspace_id, after_workspace_id in WORKSPACE_MAP[domain_id].items():
        delete_params = {"domain_id": domain_id, "workspace_id": before_workspace_id}

        if collection == "escalation_policy":
            delete_params.update({"name": "Default", "resource_group": "WORKSPACE"})

        mongo_client.delete_many(
            service,
            collection,
            delete_params,
        )


def _list_diff_workspace_names(mongo_client: MongoCustomClient, domain_id):
    before_pg_cursor = mongo_client.find(
        "IDENTITY",
        "project_group",
        {"domain_id": domain_id, "parent_project_group_id": None},
        {"_id": 0, "name": 1},
    )

    before_pg_ids = [project_group_id["name"] for project_group_id in before_pg_cursor]

    after_workspace_cursor = mongo_client.find(
        "IDENTITY-TO-BE",
        "workspace",
        {"domain_id": domain_id, "state": "ENABLED"},
        {"_id": 0, "name": 1, "workspace_id": 1},
    )

    after_workspace_ids = [
        workspace_id["name"] for workspace_id in after_workspace_cursor
    ]

    return list(set(after_workspace_ids) - set(before_pg_ids))


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.2")

    set_workspace_map(mongo_client)

    for domain_id in CHECK_DOMAINS:
        """Update operation"""

        # identity
        # change_workspace_id(mongo_client, domain_id, "IDENTITY", "project_group")
        change_workspace_id(mongo_client, domain_id, "IDENTITY", "project")
        change_workspace_id(mongo_client, domain_id, "IDENTITY", "service_account")
        change_workspace_id(mongo_client, domain_id, "IDENTITY", "role_binding")

        # monitoring
        change_workspace_id(
            mongo_client, domain_id, "MONITORING", "project_alert_config"
        )
        change_workspace_id(mongo_client, domain_id, "MONITORING", "escalation_policy")
        change_workspace_id(mongo_client, domain_id, "MONITORING", "event_rule")
        change_workspace_id(mongo_client, domain_id, "MONITORING", "webhook")
        change_workspace_id(mongo_client, domain_id, "MONITORING", "alert")
        change_workspace_id(mongo_client, domain_id, "MONITORING", "event")
        change_workspace_id(mongo_client, domain_id, "MONITORING", "note")

        # inventory
        # change_workspace_id(
        #     mongo_client, domain_id, "INVENTORY", "cloud_service", pagination=True
        # )
        change_workspace_id(mongo_client, domain_id, "INVENTORY", "note")
        change_workspace_id(mongo_client, domain_id, "INVENTORY", "collector")
        change_workspace_id(mongo_client, domain_id, "INVENTORY", "collector_rule")

        # cost-analysis
        # change_workspace_id(
        #     mongo_client, domain_id, "COST_ANALYSIS", "cost", pagination=True
        # )
        # change_workspace_id(
        #     mongo_client, domain_id, "COST_ANALYSIS", "monthly_cost", pagination=True
        # )
        change_workspace_id(mongo_client, domain_id, "COST_ANALYSIS", "budget")
        change_workspace_id(mongo_client, domain_id, "COST_ANALYSIS", "budget_usage")

        # notification
        change_workspace_id(mongo_client, domain_id, "NOTIFICATION", "project_channel")

        # statistics
        # change_workspace_id(
        #     mongo_client, domain_id, "STATISTICS", "history", pagination=True
        # )

        # secret
        change_workspace_id(mongo_client, domain_id, "SECRET", "secret")

        """Delete operation"""

        delete_resources(
            mongo_client,
            domain_id,
            "MONITORING",
            "escalation_policy",
        )
        delete_resources(
            mongo_client,
            domain_id,
            "INVENTORY",
            "region",
        )
        delete_resources(
            mongo_client,
            domain_id,
            "INVENTORY",
            "cloud_service_type",
        )
        delete_resources(
            mongo_client,
            domain_id,
            "INVENTORY",
            "cloud_service_query_set",
        )
        delete_resources(
            mongo_client,
            domain_id,
            "INVENTORY",
            "cloud_service_stats",
        )
        delete_resources(
            mongo_client,
            domain_id,
            "DASHBOARD",
            "public_dashboard",
        )
