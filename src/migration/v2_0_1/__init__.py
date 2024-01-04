import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from migration.v2_0_1 import (
    board,
    cost_analysis,
    dashboard,
    file_manager,
    identity,
    monitoring,
    notification,
    plugin,
    repository,
    secret,
    statistics,
)

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.1")

    """identity"""
    domain_items = mongo_client.find(
        "IDENTITY", "domain", {"tags.migration_complete": {"$eq": None}}, {}
    )

    for domain_info in domain_items:
        domain_id = domain_info["domain_id"]
        tags = domain_info.get("tags")

        workspace_mode = False
        if tags.get("workspace_mode") == "multi":
            workspace_mode = True

        workspace_map, project_map = identity.main(
            mongo_client, domain_id, workspace_mode
        )

        """dashboard"""
        dashboard.main(mongo_client, domain_id, project_map)

        """secret"""
        secret.main(mongo_client, domain_id, project_map)

        """monitoring"""
        monitoring.main(mongo_client, domain_id, project_map, workspace_mode)

        """notification"""
        notification.main(mongo_client, domain_id, project_map)

        # change domain tags to complete
        identity.update_domain(mongo_client, domain_id, domain_info["tags"])

    ## POST-PROCESSING
    """board"""
    board.main(mongo_client)

    """repository"""
    repository.main(mongo_client)

    # w/o dependency of project_map
    """file manager without workspace_id"""
    file_manager.file_update_fields(mongo_client)
    file_manager.file_delete_documents(mongo_client)

    """monitoring without workspace_id"""
    monitoring.event_rule_update_fields(mongo_client)
    monitoring.alert_update_fields(mongo_client)

    # drop collections
    board.drop_collections(mongo_client)
    repository.drop_collections(mongo_client)
    statistics.drop_collections(mongo_client)
    identity.drop_collections(mongo_client)
    monitoring.drop_collections(mongo_client)
    secret.drop_collections(mongo_client)
    plugin.drop_collections(mongo_client)
    dashboard.drop_collections(mongo_client)
    cost_analysis.drop_collections(mongo_client)

    #### migration2, 3 steps
    # domain_items = mongo_client.find(
    #     "IDENTITY", "domain", {"tags.migration_complete": {"$eq": None}}, {}
    # )
    # for domain_info in domain_items:
    #     domain_id = domain_info["domain_id"]
    #     tags = domain_info.get("tags")
    #
    #     workspace_mode = False
    #     if tags.get("workspace_mode") == "multi":
    #         workspace_mode = True
    #
    #     workspace_map, project_map = identity.create_workspace_project_map(
    #         mongo_client, domain_id, workspace_mode
    #     )
    #     print(workspace_map)
    #     print(project_map)

    # """inventory"""

    # step 3
    # """cost analysis"""
    # cost_analysis.main(
    #     mongo_client, domain_id, workspace_map, project_map, workspace_mode
    # )
