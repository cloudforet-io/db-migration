import logging

from datetime import datetime

from spaceone.core.utils import generate_id

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

WORKSPACE_MAP = {
    "single": {
        # {domain_id} : {workspace_id}
    },
    "multi": {
        #     {domain_id} : {
        #         {project_group_id}: {workspace_id : workspace_id, project_ids : []},
        #         {project_group_id}: {workspace_id}
        #     }
    },
}

PROJECT_MAP = {
    # {domain_id} : {
    #     {project_id} : {workspace_id}
    #     {project_id} : {workspace_id}
    #     {project_id} : {workspace_id}
    # }
}


@print_log
def identity_domain_refactoring_and_external_auth_creating(
    mongo_client: MongoCustomClient, domain_id_param
):
    domains = mongo_client.find(
        "IDENTITY", "domain", {"domain_id": domain_id_param}, {}
    )

    for domain in domains:
        domain_id = domain["domain_id"]
        domain_state = domain["state"]
        created_at = domain["created_at"]

        plugin_info = domain.get("plugin_info", {})
        if plugin_info.get("metadata"):
            if options := plugin_info.get("options"):
                plugin_info["metadata"].update(options)
        tags = domain.get("tags")

        if workspace_mode := tags.get("workspace_mode"):
            if workspace_mode == "multi":
                WORKSPACE_MAP["multi"].update({domain_id: {}})
            else:
                WORKSPACE_MAP["single"].update({domain_id: ""})

        if plugin_info and domain_state != "DELETED":
            params = {
                "domain_id": domain_id,
                "state": domain_state,
                "plugin_info": plugin_info,
                "updated_at": created_at,
            }

            mongo_client.insert_one("IDENTITY", "external_auth", params, is_new=True)

        if "config" in domain.keys() and "plugin_info" in domain.keys():
            query = {"domain_id": domain_id}
            update_params = {"$unset": {"plugin_info": 1, "config": 1, "deleted_at": 1}}
            mongo_client.update_one("IDENTITY", "domain", query, update_params)


@print_log
def identity_project_group_refactoring_and_workspace_creating(
    mongo_client: MongoCustomClient, domain_id_param
):
    project_groups = mongo_client.find(
        "IDENTITY", "project_group", {"domain_id": domain_id_param}, {}
    )
    for project_group in project_groups:
        if "parent_project_group" in project_group.keys():
            domain_id = project_group["domain_id"]
            project_group_id = project_group["project_group_id"]
            parent_project_group_id = project_group.get("parent_project_group_id")
            project_group_name = project_group["name"]

            unset_params = {"$unset": {"parent_project_group": 1, "created_by": 1}}

            set_params = {"$set": {}}

            if domain_id in WORKSPACE_MAP["multi"].keys():
                if not parent_project_group_id:
                    _create_workspace(domain_id, mongo_client, project_group_name)
                    workspace_id = _get_workspace_id(
                        domain_id, mongo_client, project_group_name
                    )
                    WORKSPACE_MAP["multi"][domain_id].update(
                        {project_group_id: workspace_id}
                    )
                    set_params["$set"].update({"workspace_id": workspace_id})
                else:
                    root_project_group = _get_root_project_group_id_by_project_group_id(
                        domain_id, parent_project_group_id, mongo_client
                    )
                    root_project_group_id = root_project_group["project_group_id"]
                    root_project_group_name = root_project_group["name"]

                    if (
                        root_project_group_id
                        in WORKSPACE_MAP["multi"][domain_id].keys()
                    ):
                        workspace_id = WORKSPACE_MAP["multi"][domain_id][
                            root_project_group_id
                        ]
                        set_params["$set"].update({"workspace_id": workspace_id})
                    else:
                        _create_workspace(
                            domain_id, mongo_client, root_project_group_name
                        )
                        workspace_id = _get_workspace_id(
                            domain_id, mongo_client, root_project_group_name
                        )
                        WORKSPACE_MAP["multi"][domain_id].update(
                            {root_project_group_id: workspace_id}
                        )
                        set_params["$set"].update({"workspace_id": workspace_id})
            else:
                workspace_id = WORKSPACE_MAP["single"].get(domain_id)
                if not workspace_id:
                    _create_workspace(domain_id, mongo_client)
                    workspace_id = _get_workspace_id(domain_id, mongo_client)
                    WORKSPACE_MAP["single"][domain_id] = workspace_id
                set_params["$set"].update({"workspace_id": workspace_id})

            mongo_client.update_one(
                "IDENTITY", "project_group", {"_id": project_group["_id"]}, set_params
            )
            mongo_client.update_one(
                "IDENTITY", "project_group", {"_id": project_group["_id"]}, unset_params
            )


@print_log
def identity_project_refactoring(mongo_client: MongoCustomClient, domain_id_param):
    projects = mongo_client.find(
        "IDENTITY", "project", {"domain_id": domain_id_param}, {}
    )

    if not projects:
        _LOGGER.error(f"domain({domain_id_param}) has no projects.")
        return

    for project in projects:
        if "project_group" in project.keys():
            set_params = {
                "$set": {
                    "project_type": "PRIVATE",
                },
                "$unset": {"project_group": 1},
            }

            project_id = project["project_id"]
            domain_id = project["domain_id"]
            project_group_id = project.get("project_group_id")
            root_project_group_id = _get_root_project_group_id_by_project_group_id(
                domain_id, project_group_id, mongo_client
            )["project_group_id"]
            workspace_id = ""

            if domain_id in WORKSPACE_MAP["multi"].keys():
                if root_project_group_id in WORKSPACE_MAP["multi"][domain_id].keys():
                    workspace_id = WORKSPACE_MAP["multi"][domain_id][
                        root_project_group_id
                    ]

            if domain_id in WORKSPACE_MAP["single"].keys():
                workspace_id = WORKSPACE_MAP["single"][domain_id]

            if not workspace_id:
                _LOGGER.error(
                    f"Project({project_id}) has no workspace_id. (project: {project})"
                )

            if domain_id not in PROJECT_MAP.keys():
                PROJECT_MAP[domain_id] = {project_id: workspace_id}
            else:
                PROJECT_MAP[domain_id].update({project_id: workspace_id})

            users = []
            if pg_role_bindings := mongo_client.find(
                "IDENTITY", "role_binding", {"project_group_id": project_group_id}, {}
            ):
                for role_binding in pg_role_bindings:
                    if (
                        role_binding["resource_type"] == "identity.User"
                        and role_binding["resource_id"] not in users
                    ):
                        users.append(role_binding["resource_id"])

            if project_role_bindings := mongo_client.find(
                "IDENTITY", "role_binding", {"project_id": project_id}, {}
            ):
                for role_binding in project_role_bindings:
                    if (
                        role_binding["resource_type"] == "identity.User"
                        and role_binding["resource_id"] not in users
                    ):
                        users.append(role_binding["resource_id"])

            set_params["$set"].update({"workspace_id": workspace_id, "users": users})

            mongo_client.update_one(
                "IDENTITY", "project", {"_id": project["_id"]}, set_params
            )


def _create_workspace(domain_id, mongo_client, project_group_name=None):
    workspaces = mongo_client.find(
        "IDENTITY", "workspace", {"domain_id": domain_id}, {}
    )
    workspace_ids = [workspace["workspace_id"] for workspace in workspaces]

    create_params = {
        "name": "Default",
        "state": "ENABLED",
        "tags": {},
        "domain_id": domain_id,
        "created_by": "SpaceONE",
        "created_at": datetime.utcnow(),
        "deleted_at": None,
    }

    workspace_id = generate_id("workspace")
    if workspace_id not in workspace_ids:
        create_params.update({"workspace_id": workspace_id})

    if project_group_name:
        create_params["name"] = project_group_name

    mongo_client.insert_one("IDENTITY", "workspace", create_params, is_new=True)


def _get_workspace_id(domain_id, mongo_client, workspace_name=None):
    query = {"domain_id": domain_id}

    if workspace_name:
        query.update({"name": workspace_name})

    workspaces = mongo_client.find("IDENTITY", "workspace", query, {})
    return [workspace["workspace_id"] for workspace in workspaces][0]


def _get_root_project_group_id_by_project_group_id(
    domain_id, project_group_id, mongo_client
):
    while project_group_id:
        project_group = mongo_client.find_one(
            "IDENTITY",
            "project_group",
            {"project_group_id": project_group_id, "domain_id": domain_id},
            {},
        )
        if parent_project_group_id := project_group.get("parent_project_group_id"):
            project_group_id = parent_project_group_id

        else:
            return project_group


@print_log
def identity_service_account_and_trusted_account_creating(
    mongo_client, domain_id_param
):
    domain_id = ""
    service_account_infos = mongo_client.find(
        "IDENTITY",
        "service_account",
        {"domain_id": domain_id_param, "service_account_type": "TRUSTED"},
        {},
    )
    for service_account_info in service_account_infos:
        domain_id = service_account_info["domain_id"]
        trusted_account_id = generate_id("ta")

        trusted_secret = mongo_client.find_one(
            "SECRET",
            "trusted_secret",
            {
                "domain_id": domain_id,
                "service_account_id": service_account_info["service_account_id"],
            },
            {},
        )

        schema_id = ""
        if trusted_secret.get("schema"):
            schema_id = _get_schema_to_schema_id(trusted_secret.get("schema"))

        trusted_account_create = {
            "trusted_account_id": trusted_account_id,
            "name": service_account_info.get("name"),
            "data": service_account_info.get("data"),
            "provider": service_account_info.get("provider"),
            "tags": service_account_info.get("tags"),
            "schema_id": schema_id,
            "trusted_secret_id": trusted_secret.get("trusted_secret_id"),
            "resource_group": "DOMAIN",
            "workspace_id": "*",
            "domain_id": service_account_info["domain_id"],
            "created_at": datetime.utcnow(),
        }

        mongo_client.insert_one(
            "IDENTITY", "trusted_account", trusted_account_create, is_new=True
        )

        mongo_client.update_many(
            "IDENTITY",
            "service_account",
            {"trusted_service_account_id": service_account_info["service_account_id"]},
            {"$set": {"trusted_service_account_id": trusted_account_id}},
        )

        mongo_client.delete_many(
            "IDENTITY", "service_account", {"_id": service_account_info["_id"]}
        )

        mongo_client.update_one(
            "SECRET",
            "trusted_secret",
            {"_id": trusted_secret["_id"]},
            {"$set": {"trusted_account_id": trusted_account_id}},
        )

        mongo_client.update_one(
            "SECRET",
            "trusted_secret",
            {"_id": trusted_secret["_id"]},
            {"$unset": {"service_account_id": 1}},
        )

    service_account_infos = mongo_client.find(
        "IDENTITY", "service_account", {"domain_id": domain_id_param}, {}
    )
    for service_account_info in service_account_infos:
        """check project_id"""
        if service_account_info.get("project_id"):
            project_id = service_account_info.get("project_id")
            workspace_id = PROJECT_MAP[domain_id].get(project_id)
        else:
            if service_account_info.get("project"):
                project_info = service_account_info.get("project")
                project_id = project_info("project_id")
                workspace_id = PROJECT_MAP[domain_id].get(project_id)
            else:
                workspace_id = list(PROJECT_MAP[domain_id].values())[0]
                project_id = _create_unmanaged_sa_project(
                    domain_id, workspace_id, mongo_client
                )

        if not project_id or not workspace_id:
            _LOGGER.error(
                f"Project({project_id}) has no workspace_id. (domain_id: {domain_id})"
            )

        set_param = {
            "$set": {"project_id": project_id, "workspace_id": workspace_id},
            "$unset": {"service_account_type": 1, "project": 1, "scope": 1},
        }

        mongo_client.update_one(
            "IDENTITY",
            "service_account",
            {"_id": service_account_info["_id"]},
            set_param,
        )


def _create_unmanaged_sa_project(domain_id, workspace_id, mongo_client):
    project_id = generate_id("project")
    name = "unmanaged-sa-project"

    create_project_param = {
        "project_id": project_id,
        "name": name,
        "project_type": "PUBLIC",
        "created_by": "spaceone",
        "workspace_id": workspace_id,
        "domain_id": domain_id,
        "created_at": datetime.utcnow(),
    }
    mongo_client.insert_one("IDENTITY", "project", create_project_param, is_new=True)
    return project_id


@print_log
def identity_role_binding_refactoring(mongo_client, domain_id_param):
    if not PROJECT_MAP.get(domain_id_param):
        _LOGGER.error(f"domain({domain_id_param}) has no projects.")
        return None

    role_binding_infos = mongo_client.find(
        "IDENTITY", "role_binding", {"domain_id": domain_id_param}, {}
    )

    for role_binding_info in role_binding_infos:
        param_role_id = role_binding_info["role_id"]

        role_info = mongo_client.find_one(
            "IDENTITY", "role", {"role_id": param_role_id}, {}
        )

        if role_info and role_info.get("role_type", "") == "DOMAIN":
            role_id = "managed-domain-admin"
            role_type = "DOMAIN_ADMIN"
            workspace_id = "*"
            resource_group = "DOMAIN"
        else:
            resource_group = "WORKSPACE"
            if not role_binding_info.get("project_group_id"):
                role_id = "managed-workspace-member"
                role_type = "WORKSPACE_MEMBER"
                workspace_id = PROJECT_MAP[domain_id_param].get(
                    role_binding_info.get("project_id")
                )
            else:
                project_group_info = mongo_client.find_one(
                    "IDENTITY",
                    "project_group",
                    {
                        "project_group_id": role_binding_info.get("project_group_id"),
                        "parent_group_id": {"$eq": None},
                    },
                    {},
                )
                workspace_id = project_group_info.get("workspace_id")
                if project_group_info:
                    role_id = "managed-workspace-owner"
                    role_type = "WORKSPACE_OWNER"
                else:
                    role_id = "managed-workspace-member"
                    role_type = "WORKSPACE_MEMBER"

        set_param = {
            "$set": {
                "user_id": role_binding_info["resource_id"],
                "role_id": role_id,
                "role_type": role_type,
                "workspace_id": workspace_id,
                "resource_group": resource_group,
            },
            "$unset": {
                "resource_type": 1,
                "resource_id": 1,
                "role": 1,
                "project": 1,
                "project_group": 1,
                "project_id": 1,
                "project_group_id": 1,
                "user": 1,
                "labels": 1,
                "tags": 1,
            },
        }

        mongo_client.update_one(
            "IDENTITY", "role_binding", {"_id": role_binding_info["_id"]}, set_param
        )


@print_log
def identity_user_refactoring(mongo_client, domain_id_param):
    user_infos = mongo_client.find(
        "IDENTITY", "user", {"domain_id": domain_id_param}, {}
    )

    role_type = "USER"
    role_id = None

    for user_info in user_infos:
        role_binding_info = mongo_client.find_one(
            "IDENTITY",
            "role_binding",
            {
                "domain_id": user_info["domain_id"],
                "user_id": user_info["user_id"],
                "role_type": "DOMAIN_ADMIN",
            },
            {},
        )
        if role_binding_info:
            role_type = "DOMAIN_ADMIN"
            role_id = role_binding_info["role_id"]

        set_param = {
            "$set": {
                "auth_type": user_info["backend"],
                "role_type": role_type,
                "role_id": role_id,
            },
            "$unset": {"user_type": 1, "backend": 1},
        }

        mongo_client.update_one(
            "IDENTITY", "user", {"_id": user_info["_id"]}, set_param
        )


def _get_schema_to_schema_id(schema):
    schema_id = None
    if schema == "azure_subscription_id":
        schema_id = "azure-secret-subscription-id"
    elif schema == "azure_client_secret":
        schema_id = "azure-secret-client-secret"
    elif schema == "google_oauth2_credentials":
        schema_id = "google-secret-oauth2-credentials"
    elif schema == "aws_assume_role" or schema == "aws_assume_role_with_external_id":
        schema_id = "aws-secret-assume-role"
    elif schema == "aws_access_key":
        schema_id = "aws-secret-access-key"
    elif schema == "google_project_id":
        schema_id = "google-secret-project-id"
    return schema_id


def drop_collections(mongo_client):
    collections = ["role", "domain_owner", "policy", "a_p_i_key"]
    for collection in collections:
        mongo_client.drop_collection("IDENTITY", collection)


@print_log
def update_domain(mongo_client, domain_id_param, domain_tags):
    set_param = {"$set": {}}
    tags = domain_tags
    tags.update({"migration_complete": True})
    set_param["$set"].update({"tags": tags})
    mongo_client.update_one(
        "IDENTITY", "domain", {"domain_id": domain_id_param}, set_param
    )


def create_workspace_project_map(
    mongo_client: MongoCustomClient, domain_id_param, workspace_mode
):
    workspace_infos = mongo_client.find(
        "IDENTITY", "workspace", {"domain_id": domain_id_param}, {}
    )
    for workspace_info in workspace_infos:
        workspace_id = workspace_info["workspace_id"]
        domain_id = workspace_info["domain_id"]

        if workspace_mode:
            if domain_id not in WORKSPACE_MAP["multi"].keys():
                WORKSPACE_MAP["multi"].update({domain_id: {}})
        else:
            if domain_id not in WORKSPACE_MAP["single"].keys():
                WORKSPACE_MAP["single"].update({domain_id: ""})

        project_infos = mongo_client.find(
            "IDENTITY", "project", {"workspace_id": workspace_id}, {}
        )
        for project_info in project_infos:
            project_group_id = project_info.get("project_group_id", None)
            project_id = project_info["project_id"]

            if workspace_mode:
                WORKSPACE_MAP["multi"][domain_id].update(
                    {project_group_id: workspace_id}
                )
            else:
                WORKSPACE_MAP["single"][domain_id] = workspace_id

            if domain_id not in PROJECT_MAP.keys():
                PROJECT_MAP[domain_id] = {project_id: workspace_id}
            else:
                PROJECT_MAP[domain_id].update({project_id: workspace_id})
    return WORKSPACE_MAP, PROJECT_MAP


def main(mongo_client, domain_id, workspace_mode):
    workspace_infos = mongo_client.find(
        "IDENTITY", "workspace", {"domain_id": domain_id}, {"_id": 1}
    )

    if len([workspace_info for workspace_info in workspace_infos]) > 0:
        return create_workspace_project_map(mongo_client, domain_id, workspace_mode)

    identity_domain_refactoring_and_external_auth_creating(mongo_client, domain_id)
    identity_project_group_refactoring_and_workspace_creating(mongo_client, domain_id)
    identity_project_refactoring(mongo_client, domain_id)
    identity_service_account_and_trusted_account_creating(mongo_client, domain_id)
    identity_role_binding_refactoring(mongo_client, domain_id)
    identity_user_refactoring(mongo_client, domain_id)

    return WORKSPACE_MAP, PROJECT_MAP
