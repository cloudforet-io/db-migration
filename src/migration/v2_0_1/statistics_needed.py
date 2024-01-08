import logging
from conf import DEFAULT_LOGGER
from pymongo import UpdateOne, DeleteOne
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def statistics_history_delete_not_exist_domain(mongo_client: MongoCustomClient):
    domain_ids = mongo_client.distinct("IDENTITY", "domain", "domain_id")
    mongo_client.delete_many(
        "STATISTICS",
        "history",
        {"domain_id": {"$nin": domain_ids}},
        {},
    )


@print_log
def statistics_history_delete_not_exist_project(mongo_client: MongoCustomClient):
    project_ids = mongo_client.distinct("IDENTITY", "project", "project_id")
    mongo_client.delete_many(
        "STATISTICS",
        "history",
        {"values.project_id": {"$nin": project_ids}},
        {},
    )


@print_log
def statistics_history_delete_not_exist_project_filed(mongo_client: MongoCustomClient):
    mongo_client.delete_many(
        "STATISTICS",
        "history",
        {"values.project_id": {"$exists": False}},
        {},
    )


@print_log
def statistics_history_add_workspace_id(mongo_client: MongoCustomClient):
    project_map = _create_project_map(mongo_client)

    for histories in mongo_client.find_by_pagination(
        "STATISTICS",
        "history",
        {"values.workspace_id": {"$exists": False}},
        {"_id": 1, "values": 1, "domain_id": 1},
        show_progress=True,
    ):
        operations = []
        for history in histories:
            values = history["values"]
            domain_id = history["domain_id"]
            project_id = values["project_id"]
            workspace_id = project_map[domain_id].get(project_id)
            if workspace_id:
                operations.append(
                    UpdateOne(
                        {"_id": history["_id"]},
                        {"$set": {"values.workspace_id": workspace_id}},
                    )
                )
        mongo_client.bulk_write("STATISTICS", "history", operations)


@print_log
def statistics_history_update_many_add_workspace_id(mongo_client: MongoCustomClient):
    histories_info = mongo_client.find(
        "STATISTICS",
        "history",
        {"values.workspace_id": {"$exists": False}},
        {"_id": 1, "values": 1, "domain_id": 1},
    )

    for history in histories_info:
        values = history["values"]
        domain_id = history["domain_id"]
        project_id = values["project_id"]
        workspace_id = mongo_client.find_one(
            "IDENTITY", "project", {"project_id": project_id}, {"workspace_id": 1}
        ).get("workspace_id")

        if workspace_id:
            mongo_client.update_many(
                "STATISTICS",
                "history",
                {"_id": history["_id"]},
                {"$set": {"values.workspace_id": workspace_id}},
            )


@print_log
def statistics_history_delete_empty_workspace_id(mongo_client: MongoCustomClient):
    mongo_client.delete_many(
        "STATISTICS",
        "history",
        {"values.workspace_id": {"$exists": False}},
        {},
    )


@print_log
def statistics_schedule_add_workspace_id(mongo_client: MongoCustomClient):
    mongo_client.delete_many(
        "STATISTICS", "schedule", {"options.resource_type": "inventory.Server"}, {}
    )

    schedules = mongo_client.find(
        "STATISTICS", "schedule", {}, {"_id": 1, "options": 1}
    )
    for schedule in schedules:
        if first_aggregate := schedule.get("options").get("aggregate"):
            for stage in first_aggregate:
                if "query" in stage:
                    keys = stage["query"]["query"]["aggregate"][0]["group"]["keys"]
                    keys.append({"name": "workspace_id", "key": "workspace_id"})
                elif "concat" in stage:
                    keys = stage["concat"]["query"]["aggregate"][0]["group"]["keys"]
                    keys.append({"name": "workspace_id", "key": "workspace_id"})
                else:
                    _LOGGER.debug(f"unknown stage: {stage}")

            set_params = {"$set": {"options.aggregate": first_aggregate}}

            mongo_client.update_one(
                "STATISTICS", "schedule", {"_id": schedule["_id"]}, set_params
            )


def _create_project_map(mongo_client):
    project_map = {}
    for project_info in mongo_client.find(
        "IDENTITY", "project", {}, {"project_id": 1, "workspace_id": 1, "domain_id": 1}
    ):
        project_map.setdefault(project_info["domain_id"], {})
        project_map[project_info["domain_id"]][
            project_info["project_id"]
        ] = project_info["workspace_id"]

    return project_map


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.2")

    # scheduler workspace_id 추가 작업
    statistics_schedule_add_workspace_id(mongo_client)

    # 사전 안 쓰이는 history 제거 작업
    statistics_history_delete_not_exist_domain(mongo_client)
    statistics_history_delete_not_exist_project(mongo_client)
    statistics_history_delete_not_exist_project_filed(mongo_client)

    # history values.workspace_id 추가 작업
    statistics_history_add_workspace_id(mongo_client)

    # 위 작업 후 누락된 workspace_id 추가 작업
    statistics_history_update_many_add_workspace_id(mongo_client)

    # 찾을 수 없는 db 제거
    # statistics_history_delete_empty_workspace_id(mongo_client)
