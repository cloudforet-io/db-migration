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
    inventory,
    cost_analysis,
)

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.1")

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

        dashboard.main(mongo_client, domain_id, project_map)
        secret.main(mongo_client, domain_id, project_map)
        monitoring.main(mongo_client, domain_id, project_map, workspace_mode)
        notification.main(mongo_client, domain_id, project_map)
        identity.update_domain(mongo_client, domain_id, domain_info["tags"])

    board.main(mongo_client)
    repository.main(mongo_client)
    file_manager.file_update_fields(mongo_client)
    file_manager.file_delete_documents(mongo_client)
    monitoring.event_rule_update_fields(mongo_client)
    monitoring.alert_update_fields(mongo_client)
    inventory.cloud_service_report_update_fields(mongo_client)
    inventory.collector_update_fields(mongo_client)
    inventory.collector_rule_update_fields(mongo_client)

    board.drop_collections(mongo_client)
    repository.drop_collections(mongo_client)
    statistics.statistics_drop_indexes(mongo_client)
    statistics.drop_collections(mongo_client)
    identity.drop_collections(mongo_client)
    monitoring.drop_collections(mongo_client)
    secret.drop_collections(mongo_client)
    plugin.plugin_drop_indexes(mongo_client)
    plugin.drop_collections(mongo_client)
    dashboard.drop_collections(mongo_client)

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

        inventory.main(mongo_client, domain_id, project_map)

        """cost analysis"""
        cost_analysis.main(
            mongo_client, domain_id, workspace_map, project_map, workspace_mode
        )

    inventory.drop_collections(mongo_client)
    cost_analysis.drop_collections(mongo_client)
