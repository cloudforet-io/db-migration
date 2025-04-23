import datetime
import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log
from datetime import datetime, timezone

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


def identity_service_account_modify_data(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "IDENTITY",
        "service_account",
        {"service_account_mgr_id": {"$exists": False}},
        {"$set": {"service_account_mgr_id": None}},
    )


@print_log
def cost_analysis_budget_modify_data(mongo_client: MongoCustomClient):
    budget_cursors = mongo_client.find("COST_ANALYSIS", "budget", {})

    for budget_info in budget_cursors:
        budget_time_unit = budget_info.get("time_unit")

        if budget_info.get("resource_group") == "WORKSPACE":
            mongo_client.delete_many(
                "COST_ANALYSIS", "budget", {"budget_id": budget_info.get("budget_id")}
            )
            mongo_client.delete_many(
                "COST_ANALYSIS",
                "budget_usage",
                {"budget_id": budget_info.get("budget_id")},
            )
        else:

            if "notifications" in budget_info:
                as_is_notifications = budget_info.get("notifications", [])
                to_be_notifications = {
                    "plans": [],
                    "recipients": {
                        "users": [],
                        "budget_manager_notification": "DISABLED",
                    },
                    "state": "DISABLED",
                }
            elif "notification" in budget_info:
                as_is_notifications = budget_info.get("notification", {})
                to_be_notifications = budget_info.get("notification", {})
                if not "recipients" in to_be_notifications:
                    to_be_notifications["recipients"] = {
                        "users": [],
                        "budget_manager_notification": "DISABLED",
                    }
            else:
                continue

            budget_start = budget_info.get("start")
            budget_end = budget_info.get("end")

            current_month = datetime.now(timezone.utc).strftime("%Y-%m")

            if budget_end < current_month:
                budget_state = "EXPIRED"
            elif budget_start > current_month:
                budget_state = "SCHEDULED"
            else:
                budget_state = "ACTIVE"

            if isinstance(as_is_notifications, list):
                for as_is_notification in as_is_notifications:
                    if "notified_months" in as_is_notification:
                        del as_is_notification["notified_months"]
                    if "as_is_notification" in as_is_notification:
                        del as_is_notification["notification_type"]

                    as_is_notification["notified"] = True
                    to_be_notifications["plans"].append(as_is_notification)
            elif isinstance(as_is_notifications, dict):
                plans = as_is_notifications.get("plans", [])
                to_be_plans = []
                for plan in plans:
                    print("plan", plan)
                    if "notified_months" in plan:
                        del plan["notified_months"]
                    if "notification_type" in plan:
                        del plan["notification_type"]

                    plan["notified"] = True
                    to_be_plans.append(plan)
                to_be_notifications["plans"] = to_be_plans

            budget_usage_cursors = mongo_client.find(
                "COST_ANALYSIS",
                "budget_usage",
                {"budget_id": budget_info.get("budget_id")},
            )
            total_usage = 0
            utilization_rate = 0
            budget_limit = budget_info.get("limit", 0)

            for budget_usage_info in budget_usage_cursors:
                budget_usage_limit = budget_usage_info.get("limit", 0)
                budget_usage_cost = budget_usage_info.get("cost", 0)

                if budget_usage_info == current_month and budget_usage_limit > 0:
                    utilization_rate = round(
                        budget_usage_cost / budget_usage_limit * 100, 2
                    )
                    budget_limit = budget_usage_limit
                elif (
                    budget_usage_info.get("date") == budget_info.get("end")
                    and budget_usage_limit > 0
                ):
                    utilization_rate = round(budget_usage_cost / budget_usage_limit)
                    budget_limit = budget_usage_limit

                total_usage += budget_usage_info.get("cost", 0)

            if budget_info.get("limit", 0) != 0 and budget_time_unit == "TOTAL":
                utilization_rate = round(
                    total_usage / budget_info.get("limit", 1) * 100, 2
                )

            set_params = {
                "notification": to_be_notifications,
                "service_account_id": None,
                "utilization_rate": utilization_rate,
                "state": budget_state,
                "budget_manager_id": None,
                "limit": budget_limit,
            }
            unset_params = {
                "data_source_id": "",
                "provider_filter": "",
                "notifications": "",
            }

            mongo_client.update_one(
                "COST_ANALYSIS",
                "budget",
                {"budget_id": budget_info.get("budget_id")},
                {
                    "$set": set_params,
                    "$unset": unset_params,
                },
            )
            mongo_client.update_many(
                "COST_ANALYSIS",
                "budget_usage",
                {"budget_id": budget_info.get("budget_id")},
                {
                    "$set": {
                        "service_account_id": None,
                    },
                    "$unset": {
                        "data_source_id": "",
                        "provider_filter": "",
                    },
                },
            )


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.5")

    # Cost Analysis Service
    cost_analysis_budget_modify_data(mongo_client)
    # Identity Service
    identity_service_account_modify_data(mongo_client)
