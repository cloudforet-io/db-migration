import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from migration.v2_0_1 import (
    identity,
    dashboard,
    secret,
    monitoring,
    notification,
    board,
    repository,
    file_manager,
    statistics,
    plugin,
    cost_analysis,
    inventory,
)

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.1")

    # Step 1: Migration for Identity, Dashboard, Secret, Monitoring, Notification
    # with dependency of project_map
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

    # Step 2: Migration for Board, Repository, Statistics, Plugin
    # without dependency of project_map
    """board"""
    board.main(mongo_client)

    """repository"""
    repository.main(mongo_client)

    """file manager without workspace_id"""
    file_manager.file_update_fields(mongo_client)
    file_manager.file_delete_documents(mongo_client)

    """monitoring without workspace_id"""
    monitoring.event_rule_update_fields(mongo_client)
    monitoring.alert_update_fields(mongo_client)

    """inventory"""
    inventory.cloud_service_report_update_fields(mongo_client)
    inventory.collector_update_fields(mongo_client)
    inventory.collector_rule_update_fields(mongo_client)

    # drop collections
    board.drop_collections(mongo_client)
    repository.drop_collections(mongo_client)
    statistics.drop_collections(mongo_client)
    identity.drop_collections(mongo_client)
    monitoring.drop_collections(mongo_client)
    secret.drop_collections(mongo_client)
    plugin.drop_collections(mongo_client)
    dashboard.drop_collections(mongo_client)
    inventory.drop_collections(mongo_client)
    cost_analysis.drop_collections(mongo_client)

    # Step 3: Migration for Inventory and Cost Analysis
    # with dependency of project_map
    domain_items = mongo_client.find("IDENTITY", "domain", {}, {})
    for domain_info in domain_items:
        domain_id = domain_info["domain_id"]
        tags = domain_info.get("tags")

        workspace_mode = False
        if tags.get("workspace_mode") == "multi":
            workspace_mode = True

        workspace_map, project_map = identity.create_workspace_project_map(
            mongo_client, domain_id, workspace_mode
        )

        """inventory"""
        inventory.main(mongo_client, domain_id, project_map)

        """cost analysis"""
        cost_analysis.main(
            mongo_client, domain_id, workspace_map, project_map, workspace_mode
        )
