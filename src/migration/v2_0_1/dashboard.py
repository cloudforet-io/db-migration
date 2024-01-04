import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def dashboard_domain_and_project_dashboard_refactoring(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    dashboards_info = []
    domain_dashboards_info = mongo_client.find(
        "DASHBOARD", "domain_dashboard", {"domain_id": domain_id}, {}
    )
    project_dashboards_info = mongo_client.find(
        "DASHBOARD", "project_dashboard", {"domain_id": domain_id}, {}
    )
    dashboards_info.extend(
        [domain_dashboard_info for domain_dashboard_info in domain_dashboards_info]
    )
    dashboards_info.extend(
        [project_dashboard_info for project_dashboard_info in project_dashboards_info]
    )

    for dashboard_info in dashboards_info:
        domain_id = dashboard_info["domain_id"]

        if "workspace_id" not in dashboard_info:
            if "viewers" == "PUBLIC":
                if "project_id" in dashboard_info:
                    dashboard_info["workspace_id"] = project_map[domain_id][
                        dashboard_info["project_id"]
                    ]
                    dashboard_info["resource_group"] = "PROJECT"
                    dashboard_info["public_dashboard_id"] = _change_prefix(
                        dashboard_info["project_dashboard_id"], "public-dash"
                    )

                    dashboard_info.pop("project_dashboard_id", 1)
                    dashboard_info.pop("user_id", 1)
                    dashboard_info.pop("viewers", 1)

                else:
                    dashboard_info["workspace_id"] = "*"
                    dashboard_info["project_id"] = "*"
                    dashboard_info["resource_group"] = "DOMAIN"
                    dashboard_info["public_dashboard_id"] = _change_prefix(
                        dashboard_info["domain_dashboard_id"], "public-dash"
                    )

                    dashboard_info.pop("domain_dashboard_id", 1)
                    dashboard_info.pop("user_id", 1)
                    dashboard_info.pop("viewers", 1)

                mongo_client.insert_one(
                    "DASHBOARD", "public_dashboard", dashboard_info, is_new=True
                )

            else:
                if "project_id" in dashboard_info:
                    dashboard_info["workspace_id"] = project_map[domain_id][
                        dashboard_info["project_id"]
                    ]
                    dashboard_info["private_dashboard_id"] = _change_prefix(
                        dashboard_info["project_dashboard_id"], "private-dash"
                    )

                    dashboard_info.pop("project_dashboard_id", 1)
                    dashboard_info.pop("viewers", 1)

                else:
                    dashboard_info["workspace_id"] = "*"
                    dashboard_info["private_dashboard_id"] = _change_prefix(
                        dashboard_info["domain_dashboard_id"], "private-dash"
                    )

                    dashboard_info.pop("domain_dashboard_id", 1)
                    dashboard_info.pop("viewers", 1)

                mongo_client.insert_one(
                    "DASHBOARD", "private_dashboard", dashboard_info, is_new=True
                )


def drop_collections(mongo_client: MongoCustomClient):
    collections = [
        "domain_dashboard",
        "project_dashboard",
        "domain_dashboard_version",
        "project_dashboard_version",
    ]

    for collection in collections:
        mongo_client.drop_collection("DASHBOARD", collection)


def _change_prefix(dashboard_id, prefix):
    _, hash_value = dashboard_id.rsplit("-", 1)
    return f"{prefix}-{hash_value}"


def main(mongo_client, domain_id, project_map):
    dashboard_domain_and_project_dashboard_refactoring(
        mongo_client, domain_id, project_map
    )
