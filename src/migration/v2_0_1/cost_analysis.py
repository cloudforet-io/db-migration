import logging
from datetime import datetime

from pymongo import UpdateOne
from spaceone.core.utils import generate_id

from conf import DEFAULT_LOGGER
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def cost_analysis_data_source_and_data_source_rule_refactoring(
    mongo_client, domain_id_param, project_map
):
    set_param = {"$set": {"resource_group": "DOMAIN", "workspace_id": "*"}}
    # check EA.  
    domain_tags = mongo_client.find_one("IDENTITY", "domain", {"domain_id": domain_id_param}, {})
    if domain_tags.get("tags").get("is_EA"):
        workspace_id = list(project_map[domain_id_param].values())[0]
        set_param = {"$set": {"resource_group": "WORKSPACE", "workspace_id": workspace_id}}
    
    #data_source
    mongo_client.update_many("COST_ANALYSIS", "data_source", {}, set_param)

    #data_source_rule
    mongo_client.update_many("COST_ANALYSIS", "data_source_rule", {}, set_param)


@print_log
def cost_analysis_budget_and_budget_usage_refactoring(
    mongo_client, domain_id_param, workspace_map, project_map, workspace_mode
):
    resource_group = ""
    workspace_id = ""

    budget_infos = mongo_client.find(
        "COST_ANALYSIS", "budget", {"domain_id": domain_id_param}, {}
    )
    for budget_info in budget_infos:
        # For idempotent
        if budget_info.get("workspace_id"):
            continue

        # if project group id exists, resource_group = WORKSPACE, or PROJECT
        if not budget_info.get("project_group_id"):
            resource_group = "PROJECT"
            workspace_id = project_map[budget_info["domain_id"]].get(
                budget_info.get("project_id", "-")
            )
        else:
            # never happen
            if not budget_info.get("project_id"):
                _LOGGER.error(
                    f"Budget({budget_info['budget_id']}) has no project and project_group_id. (domain_id: {domain_id_param})"
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

        # max under 20. so update_many.
        mongo_client.update_many(
            "COST_ANALYSIS",
            "budget_usage",
            {"budget_id": budget_info["budget_id"]},
            set_params,
        )


@print_log
def cost_analysis_cost_query_set_refactoring(
    mongo_client, domain_id_param, project_map
):
    
    cost_query_sets_info = mongo_client.find(
        "COST_ANALYSIS", "cost_query_set", {"domain_id": domain_id_param}, {}
    )

    for cost_query_set_info in cost_query_sets_info:
        # For idempotent
        if cost_query_set_info.get("workspace_id"):
            continue

        for workspace_id in list(project_map(list(project_map[cost_query_set_info['domain_id']].values()))):
            _create_cost_query_set(mongo_client, cost_query_set_info, workspace_id)
        
        mongo_client.delete_many("COST_ANALYSIS", "cost_query_set", {'_id':cost_query_set_info['_id']})


@print_log
def cost_analysis_cost_refactoring(
    mongo_client, domain_id_param, workspace_map, project_map, workspace_mode
):
    workspace_id = None
    operations = []
    item_count = 0
    is_EA = False

    domain_tags = mongo_client.find_one("IDENTITY", "domain", {"domain_id": domain_id_param}, {})
    if domain_tags.get("tags").get("is_EA"):
        is_EA = True
    
    for costs_info in mongo_client.find_by_pagination(
        "COST_ANALYSIS", "cost", {"domain_id": domain_id_param}
        , {"_id": 1, "workspace_id": 1, "project_id": 1, "project_group_id": 1, "domain_id": 1}
    ):
        operations = []
        for cost_info in costs_info:
            # For idempotent
            if cost_info.get("workspace_id"):
                continue

            if cost_info.get("project_id"):
                # if project_id is not null
                workspace_id = project_map[cost_info["domain_id"]].get(
                    cost_info.get("project_id")
                )
            elif cost_info.get("project_group_id"):
                # if project_id is null, project_group_id is not null
                if workspace_mode:
                    workspace_id = workspace_map["multi"][cost_info["domain_id"]].get(
                        cost_info.get("project_group_id")
                    )
                else:
                    workspace_id = workspace_map["single"][cost_info["domain_id"]]
            else:
                # if not exists both project_id, project_group_id
                # check EA. if EA, then default workspace_id
                if is_EA:
                    workspace_id = list(project_map[domain_id_param].values())[0]
            set_params = {
                "$set": {"workspace_id": workspace_id},
                "$unset": {"project_group_id": 1},
            }

            operations.append(UpdateOne({"_id": cost_info["_id"]}, set_params))

            item_count += 1

        mongo_client.bulk_write("COST_ANALYSIS", "cost", operations)
        _LOGGER.info(f"Total Count : {item_count}")


@print_log
def cost_analysis_monthly_cost_refactoring(
    mongo_client, domain_id_param, workspace_map, project_map, workspace_mode
):
    workspace_id = None
    operations = []
    item_count = 0
    is_EA = False

    domain_tags = mongo_client.find_one("IDENTITY", "domain", {"domain_id": domain_id_param}, {})
    if domain_tags.get("tags").get("is_EA"):
        is_EA = True

    for monthly_costs_info in mongo_client.find_by_pagination(
        "COST_ANALYSIS", "monthly_cost", {"domain_id": domain_id_param}
        , {"_id": 1, "workspace_id": 1, "project_id": 1, "project_group_id": 1, "domain_id": 1}
    ):
        operations = []

        for monthly_cost_info in monthly_costs_info: 
            # For idempotent
            if monthly_cost_info.get("workspace_id"):
                continue

            if monthly_cost_info.get("project_id"):
                # if project_id is not null
                workspace_id = project_map[monthly_cost_info["domain_id"]].get(
                    monthly_cost_info.get("project_id")
                )
            elif monthly_cost_info.get("project_group_id"):
                # if project_id is null, project_group_id is not null
                if workspace_mode:
                    workspace_id = workspace_map["multi"][monthly_cost_info["domain_id"]].get(
                        monthly_cost_info.get("project_group_id")
                    )
                else:
                    workspace_id = workspace_map["single"][monthly_cost_info["domain_id"]]
            else:
                # if not exists both project_id, project_group_id
                # check EA. if EA, then default workspace_id
                if is_EA:
                    workspace_id = list(project_map[domain_id_param].values())[0]
            set_params = {
                "$set": {"workspace_id": workspace_id},
                "$unset": {"project_group_id": 1},
            }

            operations.append(UpdateOne({"_id": monthly_cost_info["_id"]}, set_params))

            item_count += 1
        
        mongo_client.bulk_write("COST_ANALYSIS", "monthly_cost", operations)
        _LOGGER.info(f"Total Count : {item_count}")


@print_log
def drop_collections(mongo_client):
    # drop role after refactoring role_binding
    collections = ["job", "job_task", "cost_query_history"]
    for collection in collections:
        mongo_client.drop_collection("COST_ANALYSIS", collection)


def _create_cost_query_set(mongo_client, cost_query_set_info, workspace_id):
    cost_query_set_id = generate_id("query")
    
    create_cost_query_set_param = {
            "cost_query_set_id": cost_query_set_id
            , "name": cost_query_set_info["name"]
            , "options": cost_query_set_info["options"]
            , "tags": cost_query_set_info["tags"]
            , "user_id": cost_query_set_info["user_id"]
            , "data_source_id": cost_query_set_info["data_source_id"]
            , "workspace_id": workspace_id
            , "domain_id": cost_query_set_info["domain_id"]
            , "created_at": datetime.utcnow(),
    }

    mongo_client.insert_one("COST_ANALYSIS", "cost_query_set", create_cost_query_set_param, {})


def main(mongo_client, domain_id_param, workspace_map, project_map, workspace_mode):
    # data_source, data_source_rule
    cost_analysis_data_source_and_data_source_rule_refactoring(
        mongo_client, domain_id_param, project_map
    )

    # budget, budget_usage
    cost_analysis_budget_and_budget_usage_refactoring(
        mongo_client, domain_id_param, workspace_map, project_map, workspace_mode
    )

    # cost_query_set
    cost_analysis_cost_query_set_refactoring(
        mongo_client, domain_id_param, project_map
    )

    # cost
    cost_analysis_cost_refactoring(
        mongo_client, domain_id_param, workspace_map, project_map, workspace_mode
    )

    # monthly_cost
    cost_analysis_monthly_cost_refactoring(
        mongo_client, domain_id_param, workspace_map, project_map, workspace_mode
    )

