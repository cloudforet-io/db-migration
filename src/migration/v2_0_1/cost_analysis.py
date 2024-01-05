import logging
from datetime import datetime

from pymongo import UpdateOne
from spaceone.core.utils import generate_id

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def cost_analysis_data_source_and_data_source_rule_refactoring(
    mongo_client, domain_id, project_map
):
    set_param = {"$set": {"resource_group": "DOMAIN", "workspace_id": "*"}}
    domain_tags = mongo_client.find_one(
        "IDENTITY", "domain", {"domain_id": domain_id}, {}
    )
    if domain_tags.get("tags").get("is_EA"):
        workspace_id = list(project_map[domain_id].values())[0]
        set_param = {
            "$set": {"resource_group": "WORKSPACE", "workspace_id": workspace_id}
        }

    mongo_client.update_many("COST_ANALYSIS", "data_source", {}, set_param)
    mongo_client.update_many("COST_ANALYSIS", "data_source_rule", {}, set_param)


@print_log
def cost_analysis_budget_and_budget_usage_refactoring(
    mongo_client, domain_id, workspace_map, project_map, workspace_mode
):
    resource_group = ""
    workspace_id = ""

    budget_infos = mongo_client.find(
        "COST_ANALYSIS", "budget", {"domain_id": domain_id}, {}
    )
    for budget_info in budget_infos:
        if budget_info.get("workspace_id"):
            continue

        if not budget_info.get("project_group_id"):
            resource_group = "PROJECT"
            workspace_id = project_map[budget_info["domain_id"]].get(
                budget_info.get("project_id", "-")
            )
        else:
            if not budget_info.get("project_id"):
                _LOGGER.error(
                    f"Budget({budget_info['budget_id']}) has no project and project_group_id. (domain_id: {domain_id})"
                )

            resource_group = "WORKSPACE"
            if workspace_mode:
                workspace_id = workspace_map["multi"][budget_info["domain_id"]].get(
                    budget_info.get("project_group_id")
                )
            else:
                workspace_id = workspace_map["single"][budget_info["domain_id"]]

        set_params = {
            "$set": {"resource_group": resource_group, "workspace_id": workspace_id},
            "$unset": {"project_group_id": 1},
        }

        mongo_client.update_one(
            "COST_ANALYSIS", "budget", {"_id": budget_info["_id"]}, set_params
        )

        mongo_client.update_many(
            "COST_ANALYSIS",
            "budget_usage",
            {"budget_id": budget_info["budget_id"]},
            set_params,
        )


@print_log
def cost_analysis_cost_query_set_refactoring(mongo_client, domain_id, project_map):
    cost_query_sets_info = mongo_client.find(
        "COST_ANALYSIS", "cost_query_set", {"domain_id": domain_id}, {}
    )

    for cost_query_set_info in cost_query_sets_info:
        if cost_query_set_info.get("workspace_id"):
            continue

        for workspace_id in list(
            dict.fromkeys(list(project_map[cost_query_set_info["domain_id"]].values()))
        ):
            _create_cost_query_set(mongo_client, cost_query_set_info, workspace_id)

        mongo_client.delete_many(
            "COST_ANALYSIS", "cost_query_set", {"_id": cost_query_set_info["_id"]}
        )


@print_log
def cost_analysis_cost_refactoring(
    mongo_client, domain_id, workspace_map, project_map, workspace_mode
):
    workspace_id = None
    item_count = 0
    is_EA = False

    domain_tags = mongo_client.find_one(
        "IDENTITY", "domain", {"domain_id": domain_id}, {}
    )
    if domain_tags.get("tags").get("is_EA"):
        is_EA = True

    for costs_info in mongo_client.find_by_pagination(
        "COST_ANALYSIS",
        "cost",
        {"domain_id": domain_id},
        {
            "_id": 1,
            "workspace_id": 1,
            "project_id": 1,
            "project_group_id": 1,
            "domain_id": 1,
        },
        show_progress=True,
    ):
        operations = []
        for cost_info in costs_info:
            if cost_info.get("workspace_id"):
                continue

            if cost_info.get("project_id"):
                workspace_id = project_map[cost_info["domain_id"]].get(
                    cost_info.get("project_id")
                )
            elif cost_info.get("project_group_id"):
                if workspace_mode:
                    workspace_id = workspace_map["multi"][cost_info["domain_id"]].get(
                        cost_info.get("project_group_id")
                    )
                else:
                    workspace_id = workspace_map["single"][cost_info["domain_id"]]
            else:
                if is_EA:
                    workspace_id = list(project_map[domain_id].values())[0]
            set_params = {
                "$set": {"workspace_id": workspace_id},
                "$unset": {"project_group_id": 1},
            }

            operations.append(UpdateOne({"_id": cost_info["_id"]}, set_params))

            item_count += 1

        mongo_client.bulk_write("COST_ANALYSIS", "cost", operations)


@print_log
def cost_analysis_monthly_cost_refactoring(
    mongo_client, domain_id, workspace_map, project_map, workspace_mode
):
    workspace_id = None
    item_count = 0
    is_EA = False

    domain_tags = mongo_client.find_one(
        "IDENTITY", "domain", {"domain_id": domain_id}, {}
    )
    if domain_tags.get("tags").get("is_EA"):
        is_EA = True

    for monthly_costs_info in mongo_client.find_by_pagination(
        "COST_ANALYSIS",
        "monthly_cost",
        {"domain_id": domain_id},
        {
            "_id": 1,
            "workspace_id": 1,
            "project_id": 1,
            "project_group_id": 1,
            "domain_id": 1,
        },
        show_progress=True,
    ):
        operations = []

        for monthly_cost_info in monthly_costs_info:
            if monthly_cost_info.get("workspace_id"):
                continue

            if monthly_cost_info.get("project_id"):
                workspace_id = project_map[monthly_cost_info["domain_id"]].get(
                    monthly_cost_info.get("project_id")
                )
            elif monthly_cost_info.get("project_group_id"):
                if workspace_mode:
                    workspace_id = workspace_map["multi"][
                        monthly_cost_info["domain_id"]
                    ].get(monthly_cost_info.get("project_group_id"))
                else:
                    workspace_id = workspace_map["single"][
                        monthly_cost_info["domain_id"]
                    ]
            else:
                if is_EA:
                    workspace_id = list(project_map[domain_id].values())[0]
            set_params = {
                "$set": {"workspace_id": workspace_id},
                "$unset": {"project_group_id": 1},
            }

            operations.append(UpdateOne({"_id": monthly_cost_info["_id"]}, set_params))

            item_count += 1

        mongo_client.bulk_write("COST_ANALYSIS", "monthly_cost", operations)


@print_log
def drop_collections(mongo_client):
    collections = ["job", "job_task", "cost_query_history"]
    for collection in collections:
        mongo_client.drop_collection("COST_ANALYSIS", collection)


def _create_cost_query_set(mongo_client, cost_query_set_info, workspace_id):
    cost_query_set_id = generate_id("query")

    create_cost_query_set_param = {
        "cost_query_set_id": cost_query_set_id,
        "name": cost_query_set_info["name"],
        "options": cost_query_set_info["options"],
        "tags": cost_query_set_info["tags"],
        "user_id": cost_query_set_info["user_id"],
        "data_source_id": cost_query_set_info["data_source_id"],
        "workspace_id": workspace_id,
        "domain_id": cost_query_set_info["domain_id"],
        "created_at": datetime.utcnow(),
    }

    mongo_client.insert_one(
        "COST_ANALYSIS", "cost_query_set", create_cost_query_set_param, {}
    )


@print_log
def cost_analysis_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("COST_ANALYSIS", "*")


def main(mongo_client, domain_id, workspace_map, project_map, workspace_mode):
    cost_analysis_drop_indexes(mongo_client)
    cost_analysis_data_source_and_data_source_rule_refactoring(
        mongo_client, domain_id, project_map
    )
    cost_analysis_budget_and_budget_usage_refactoring(
        mongo_client, domain_id, workspace_map, project_map, workspace_mode
    )
    cost_analysis_cost_query_set_refactoring(mongo_client, domain_id, project_map)
    cost_analysis_cost_refactoring(
        mongo_client, domain_id, workspace_map, project_map, workspace_mode
    )
    cost_analysis_monthly_cost_refactoring(
        mongo_client, domain_id, workspace_map, project_map, workspace_mode
    )
