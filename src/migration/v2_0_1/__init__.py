import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from migration.v2_0_1 import board, repository, statistics, identity, notification

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
        workspace_map, project_map = identity.main(mongo_client, domain_id)

        """file manager"""

        """dashboard"""

        """monitoring"""

        """cost analysis"""

        """inventory"""

        """secret"""

        """notification"""
        notification.main(mongo_client, domain_id, project_map)

        # change domain tags to complete
        identity.update_domain(mongo_client, domain_id, domain_info['tags'])

    ## POST-PROCESSING
    # drop collections 
    identity.drop_collections(mongo_client)