import logging
from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log
from datetime import datetime
from spaceone.core.utils import generate_id

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

WORKSPACE_MAP = {
    "single": {
        # {domain_id} : {workspace_id}
    },
    "multi": {
        #     {domain_id} : {
        #         {project_group_id}: {workspace_id : workspace_id
        #         project_ids : []},
        #         {project_group_id}: {workspace_id}
        #     }
    }
}

PROJECT_MAP = {
    # {domain_id} : {
    #     {project_id} : {workspace_id}
    #     {project_id} : {workspace_id}
    #     {project_id} : {workspace_id}
    # }
}


# Identity service
@print_log
def identity_domain_refactoring_and_external_auth_creating(mongo_client: MongoCustomClient):
    domains = mongo_client.find('IDENTITY', 'domain', {}, {})

    for domain in domains:
        domain_id = domain['domain_id']
        domain_state = domain['state']
        created_at = domain['created_at']

        plugin_info = domain.get('plugin_info', {})
        tags = domain.get('tags')

        if workspace_mode := tags.get("workspace_mode"):
            if workspace_mode == "multi":
                WORKSPACE_MAP["multi"].update({domain_id: {}})
            else:
                WORKSPACE_MAP["single"].update({domain_id: ''})

        if plugin_info and domain_state != 'DELETED':
            params = {
                'domain_id': domain_id,
                'state': domain_state,
                'plugin_info': plugin_info,
                'updated_at': created_at
            }
            # TODO : plugin_info 스펙 변경이 있는 지 확인이 필요함
            mongo_client.insert_one('IDENTITY', 'external_auth', params, is_new=True)

        if 'config' in domain.keys() and 'plugin_info' in domain.keys():
            query = {'domain_id': domain_id}
            update_params = {
                '$unset': {
                    'plugin_info': 1,
                    'config': 1,
                    'deleted_at': 1
                }
            }
            mongo_client.update_one('IDENTITY', 'domain', query, update_params)


@print_log
def identity_project_group_refactoring_and_workspace_creating(mongo_client: MongoCustomClient):
    project_groups = mongo_client.find('IDENTITY', 'project_group', {}, {})
    for project_group in project_groups:

        if "parent_project_group" in project_group.keys():
            domain_id = project_group['domain_id']
            project_group_id = project_group['project_group_id']
            parent_project_group_id = project_group.get('parent_project_group_id')
            project_group_name = project_group['name']

            unset_params = {
                "$unset": {
                    "parent_project_group": 1,
                    "created_by": 1
                }
            }

            set_params = {
                "$set": {}
            }

            if domain_id in WORKSPACE_MAP['multi'].keys():
                if not parent_project_group_id:
                    _create_workspace(domain_id, mongo_client, project_group_name)
                    workspace_id = _get_workspace_id(domain_id, mongo_client, project_group_name)
                    WORKSPACE_MAP['multi'][domain_id].update(
                        {project_group_id: workspace_id}
                    )
                    set_params["$set"].update({
                        "workspace_id": workspace_id
                    })
                else:
                    root_project_group = _get_root_project_group_id_by_project_group_id(
                        domain_id, parent_project_group_id, mongo_client)
                    root_project_group_id = root_project_group['project_group_id']
                    root_project_group_name = root_project_group['name']

                    if root_project_group_id in WORKSPACE_MAP['multi'][domain_id].keys():
                        workspace_id = WORKSPACE_MAP['multi'][domain_id][root_project_group_id]
                        set_params["$set"].update({
                            "workspace_id": workspace_id
                        })
                    else:
                        _create_workspace(domain_id, mongo_client, root_project_group_name)
                        workspace_id = _get_workspace_id(domain_id, mongo_client, root_project_group_name)
                        WORKSPACE_MAP['multi'][domain_id].update(
                            {root_project_group_id: workspace_id}
                        )
                        set_params["$set"].update({
                            "workspace_id": workspace_id
                        })
            else:
                workspace_id = WORKSPACE_MAP['single'].get(domain_id)
                if not workspace_id:
                    _create_workspace(domain_id, mongo_client)
                    workspace_id = _get_workspace_id(domain_id, mongo_client)
                    WORKSPACE_MAP['single'][domain_id] = workspace_id
                set_params["$set"].update({
                    "workspace_id": workspace_id
                })

            mongo_client.update_one('IDENTITY', 'project_group', {"_id": project_group["_id"]}, set_params)
            mongo_client.update_one('IDENTITY', 'project_group', {"_id": project_group["_id"]}, unset_params)


def identity_project_refactoring(mongo_client: MongoCustomClient):
    projects = mongo_client.find('IDENTITY', 'project', {}, {})
    for project in projects:
        if "project_group" in project.keys():

            unset_params = {
                "$unset": {
                    "project_group": 1
                }
            }

            set_params = {
                "$set": {
                    "project_type": "PRIVATE",
                }
            }

            project_id = project['project_id']
            domain_id = project['domain_id']
            project_group_id = project.get('project_group_id')
            root_project_group_id = _get_root_project_group_id_by_project_group_id(
                domain_id, project_group_id, mongo_client
            )['project_group_id']
            workspace_id = ''

            if domain_id in WORKSPACE_MAP['multi'].keys():
                if root_project_group_id in WORKSPACE_MAP['multi'][domain_id].keys():
                    workspace_id = WORKSPACE_MAP['multi'][domain_id][root_project_group_id]

            if domain_id in WORKSPACE_MAP['single'].keys():
                workspace_id = WORKSPACE_MAP['single'][domain_id]

            if not workspace_id:
                _LOGGER.error(f'Project({project_id}) has no workspace_id. (project: {project})')

            if domain_id not in PROJECT_MAP.keys():
                PROJECT_MAP[domain_id] = {project_id: workspace_id}
            else:
                PROJECT_MAP[domain_id].update({project_id: workspace_id})

            users = []
            if pg_role_bindings := mongo_client.find('IDENTITY', 'role_binding', {'project_group_id': project_group_id},
                                                     {}):
                for role_binding in pg_role_bindings:
                    if role_binding['resource_type'] == 'identity.User' and role_binding['resource_id'] not in users:
                        users.append(role_binding['resource_id'])

            if project_role_bindings := mongo_client.find('IDENTITY', 'role_binding', {'project_id': project_id}, {}):
                for role_binding in project_role_bindings:
                    if role_binding['resource_type'] == 'identity.User' and role_binding['resource_id'] not in users:
                        users.append(role_binding['resource_id'])

            set_params["$set"].update({
                "workspace_id": workspace_id, "users": users
            })

            mongo_client.update_one('IDENTITY', 'project', {"_id": project["_id"]}, set_params)


def _create_workspace(domain_id, mongo_client, project_group_name=None):
    workspaces = mongo_client.find("IDENTITY", "workspace", {"domain_id": domain_id}, {})
    workspace_ids = [workspace['workspace_id'] for workspace in workspaces]

    create_params = {
        "name": "Default",
        "state": "ENABLED",
        "tags": {},
        "domain_id": domain_id,
        "created_by": "SpaceONE",
        "created_at": datetime.utcnow(),
        "deleted_at": None
    }

    workspace_id = generate_id("workspace")
    if workspace_id not in workspace_ids:
        create_params.update({"workspace_id": workspace_id})

    if project_group_name:
        create_params["name"] = project_group_name

    mongo_client.insert_one("IDENTITY", "workspace", create_params, is_new=True)


def _get_workspace_id(domain_id, mongo_client, workspace_name=None):
    query = {
        "domain_id": domain_id
    }

    if workspace_name:
        query.update({"name": workspace_name})

    workspaces = mongo_client.find("IDENTITY", "workspace", query, {})
    return [workspace["workspace_id"] for workspace in workspaces][0]


def _get_root_project_group_id_by_project_group_id(domain_id, project_group_id, mongo_client):
    while project_group_id:
        project_group = mongo_client.find_one('IDENTITY', 'project_group',
                                              {"project_group_id": project_group_id, "domain_id": domain_id}, {})
        if parent_project_group_id := project_group.get('parent_project_group_id'):
            project_group_id = parent_project_group_id

        else:
            return project_group


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, 'v2.0.1')

    identity_domain_refactoring_and_external_auth_creating(mongo_client)
    identity_project_group_refactoring_and_workspace_creating(mongo_client)
    print(WORKSPACE_MAP)
    identity_project_refactoring(mongo_client)
    print(">>>", PROJECT_MAP)
