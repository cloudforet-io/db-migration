import logging
from datetime import datetime

from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def notification_project_channel_refactoring(
    mongo_client: MongoCustomClient, domain_id_param, project_map
):
    operations = []

    project_channel_infos = mongo_client.find(
        "NOTIFICATION", "project_channel", {"domain_id": domain_id_param}, {}
    )

    for project_channel_info in project_channel_infos:
        if project_channel_info.get("workspace_id"):
            continue

        workspace_id = project_map[project_channel_info["domain_id"]].get(
            project_channel_info["project_id"]
        )
        operations.append(
            UpdateOne(
                {"_id": project_channel_info["_id"]},
                {"$set": {"workspace_id": workspace_id}},
            )
        )

    mongo_client.bulk_write("NOTIFICATION", "project_channel", operations)


@print_log
def notification_user_channel_migration(
    mongo_client: MongoCustomClient, domain_id_param
):
    user_channel_infos = mongo_client.find(
        "NOTIFICATION",
        "user_channel",
        {"domain_id": domain_id_param, "secret_id": {"$exists": True}},
        {},
    )
    for user_channel_info in user_channel_infos:
        if user_channel_info.get("secret_id"):
            secret_info = mongo_client.find_one(
                "SECRET",
                "secret",
                {"secret_id": user_channel_info.get("secret_id")},
                {},
            )
            create_user_secret_param = {
                "user_secret_id": secret_info["secret_id"],
                "name": secret_info["name"],
                "schema_id": secret_info["schema_id"],
                "provider": secret_info["provider"],
                "tags": secret_info["tags"],
                "encrypted": secret_info["encrypted"],
                "encrypt_options": secret_info["encrypt_options"],
                "user_id": user_channel_info["user_id"],
                "domain_id": secret_info["domain_id"],
                "created_at": datetime.utcnow(),
            }
            mongo_client.insert_one(
                "SECRET", "user_secret", create_user_secret_param, is_new=True
            )
            mongo_client.delete_many("SECRET", "secret", {"_id": secret_info["_id"]})
            mongo_client.update_one(
                "NOTIFICATION",
                "user_channel",
                {"_id": user_channel_info["_id"]},
                {"$rename": {"secret_id": "user_secret_id"}},
            )


@print_log
def notification_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("NOTIFICATION", "*")


def main(mongo_client, domain_id_param, project_map):
    notification_drop_indexes(mongo_client)
    notification_project_channel_refactoring(mongo_client, domain_id_param, project_map)
    notification_user_channel_migration(mongo_client, domain_id_param)
