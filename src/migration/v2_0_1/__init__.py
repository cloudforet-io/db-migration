import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from migration.v2_0_1 import board, repository, statistics, identity, notification, cost_analysis, secret

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.1")

    """board"""
    board.main(mongo_client)

    """repository"""
    repository.main(mongo_client)

    """statistics"""
    statistics.main(mongo_client)

    # config(No Changes)

    # plugin(No Changes)

    """identity"""
    domain_items = mongo_client.find("IDENTITY", "domain", {'tags.migration_complete':{'$eq': None}}, {})
    for domain_info in domain_items:
        domain_id = domain_info["domain_id"]
        tags = domain_info.get("tags")
        workspace_mode = False

        if workspace_mode := tags.get("workspace_mode"):
            if workspace_mode == "multi":
                workspace_mode = True
            
        workspace_map, project_map = identity.main(mongo_client, domain_id, workspace_mode)

        """file manager"""

        """dashboard"""

        """monitoring"""

        """secret"""

        """notification"""
        notification.main(mongo_client, domain_id, project_map)

        # change domain tags to complete
        identity.update_domain(mongo_client, domain_id, domain_info['tags'])

        """inventory"""

        """cost analysis"""
        cost_analysis.main(mongo_client, domain_id, workspace_map, project_map, workspace_mode)

    ## POST-PROCESSING
    # drop collections 
    identity.drop_collections(mongo_client)
    secret.drop_collections(mongo_client)
    cost_analysis.drop_collections(mongo_client)

    #### migration2, 3 steps
    # domain_items = mongo_client.find("IDENTITY", "domain", {'tags.migration_complete':{'$eq': None}}, {})
    # for domain_info in domain_items:
    #     domain_id = domain_info["domain_id"]
    #     tags = domain_info.get("tags")
    #     workspace_mode = False
    #     workspace_map, project_map = identity._create_workspace_project_map(mongo_client, domain_id, workspace_mode)
    #     print(workspace_map)
    #     print(project_map)