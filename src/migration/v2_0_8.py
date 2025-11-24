import logging

from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def identity_role_page_access_refactoring(mongo_client: MongoCustomClient):
    target_values = ["iam:writable.user_group", "iam:readonly.user_group"]
    replace_map = {
        "iam:writable.user_group": "alert_manager:writable.user_group",
        "iam:readonly.user_group": "alert_manager:readonly.user_group",
    }

    roles_info = mongo_client.find(
        "IDENTITY",
        "role",
        {"page_access": {"$in": target_values}},
        {"_id": 1, "page_access": 1},
    )

    operations = []
    for role_info in roles_info:
        page_access = role_info.get("page_access", [])
        updated_page_access = [
            replace_map.get(access, access) for access in page_access
        ]

        operations.append(
            UpdateOne(
                {"_id": role_info["_id"]},
                {"$set": {"page_access": updated_page_access}},
            )
        )

    if operations:
        mongo_client.bulk_write("IDENTITY", "role", operations)
        _LOGGER.info(f"Updated {len(operations)} role documents")


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.8")

    # Identity Service
    identity_role_page_access_refactoring(mongo_client)
