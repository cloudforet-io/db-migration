import logging
from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log
from spaceone.core.utils import generate_id

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def dashboard_refactoring(mongo_client):
    project_map = _create_project_map(mongo_client)

    dashboard_drop_indexes(mongo_client)

    for domain in mongo_client.find("IDENTITY", "domain", {}, {"domain_id": 1}):
        _create_public_and_private_dashboard(
            mongo_client, domain["domain_id"], project_map
        )

    dashboard_drop_collections(mongo_client)


def dashboard_domain_to_workspace(mongo_client: MongoCustomClient):
    project_map = _create_project_map(mongo_client)

    for domain in mongo_client.find("IDENTITY", "domain", {}, {"domain_id": 1}):
        _apply_domain_to_workspace_dashboard(
            mongo_client, domain["domain_id"], project_map
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


@print_log
def _create_public_and_private_dashboard(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    dashboards_info = []
    domain_dashboards_info = mongo_client.find(
        "DASHBOARD",
        "domain_dashboard",
        {"domain_id": domain_id, "workspace_id": {"$exists": False}},
        {},
    )
    project_dashboards_info = mongo_client.find(
        "DASHBOARD",
        "project_dashboard",
        {"domain_id": domain_id, "workspace_id": {"$exists": False}},
        {},
    )
    dashboards_info.extend(
        [domain_dashboard_info for domain_dashboard_info in domain_dashboards_info]
    )
    dashboards_info.extend(
        [project_dashboard_info for project_dashboard_info in project_dashboards_info]
    )

    for dashboard_info in dashboards_info:
        domain_id = dashboard_info["domain_id"]

        if dashboard_info["viewers"] == "PUBLIC":
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


@print_log
def _apply_domain_to_workspace_dashboard(
    mongo_client: MongoCustomClient, domain_id, project_map
):
    workspace_ids = []
    if projects_info := project_map.get(domain_id):
        for project_id in projects_info.keys():
            workspace_ids.append(projects_info[project_id])
    workspace_ids = list(set(workspace_ids))
    workspace_count = len(workspace_ids)

    domain_dashboards_info = mongo_client.find(
        "DASHBOARD",
        "public_dashboard",
        {"domain_id": domain_id, "resource_group": "DOMAIN"},
        {},
    )

    for domain_dashboard_info in domain_dashboards_info:
        public_dashboard_id = domain_dashboard_info["public_dashboard_id"]

        if workspace_count == 0:
            continue

        elif workspace_count == 1:
            update_params = {
                "$set": {
                    "resource_group": "WORKSPACE",
                    "workspace_id": workspace_ids[0],
                }
            }
            mongo_client.update_one(
                "DASHBOARD",
                "public_dashboard",
                {"public_dashboard_id": public_dashboard_id},
                update_params,
            )

        else:
            for workspace_id in workspace_ids:
                insert_params = {
                    "name": domain_dashboard_info["name"],
                    "version": domain_dashboard_info.get("version"),
                    "layouts": domain_dashboard_info.get("layouts"),
                    "variables": domain_dashboard_info.get("variables"),
                    "settings": domain_dashboard_info.get("settings"),
                    "variables_schema": domain_dashboard_info.get("variables_schema"),
                    "labels": domain_dashboard_info.get("labels"),
                    "tags": domain_dashboard_info.get("tags"),
                    "domain_id": domain_id,
                    "workspace_id": workspace_id,
                    "project_id": "*",
                    "resource_group": "WORKSPACE",
                    "public_dashboard_id": generate_id("public-dash", 6),
                    "created_at": domain_dashboard_info["created_at"],
                    "updated_at": domain_dashboard_info["updated_at"]
                    if "updated_at" in domain_dashboard_info
                    else domain_dashboard_info["created_at"],
                }
                mongo_client.insert_one("DASHBOARD", "public_dashboard", insert_params)


@print_log
def dashboard_public_dashboard_resource_group_domain(mongo_client: MongoCustomClient):
    mongo_client.delete_many(
        "DASHBOARD", "public_dashboard", {"resource_group": "DOMAIN"}, {}
    )


def dashboard_drop_collections(mongo_client: MongoCustomClient):
    collections = [
        "domain_dashboard",
        "domain_dashboard_version",
        "project_dashboard",
        "project_dashboard_version",
    ]
    for collection in collections:
        mongo_client.drop_collection("DASHBOARD", collection)


@print_log
def dashboard_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("DASHBOARD", "*")


def _change_prefix(dashboard_id, prefix):
    _, hash_value = dashboard_id.rsplit("-", 1)
    return f"{prefix}-{hash_value}"


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.3")

    # step 1 : public, private dashboard 생성
    dashboard_refactoring(mongo_client)

    # step 2 : resource_group DOMAIN -> WORKSPACE 변경
    dashboard_domain_to_workspace(mongo_client)

    # step 3 : public_dashboard resource_group DOMAIN 제거
    dashboard_public_dashboard_resource_group_domain(mongo_client)
