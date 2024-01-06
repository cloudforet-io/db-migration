import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def repository_plugin_update_drop_fields(mongo_client: MongoCustomClient):
    plugin_infos = mongo_client.find("REPOSITORY", "plugin", {}, {})
    for plugin_info in plugin_infos:
        if "repository_id" in plugin_info.keys():
            repositories_info = mongo_client.find(
                "REPOSITORY",
                "repository",
                {
                    "repository_id": plugin_info["repository_id"],
                    "repository_type": "local",
                },
                {"_id": 1},
            )
            if len([repository_info for repository_info in repositories_info]) > 0:
                mongo_client.update_one(
                    "REPOSITORY",
                    "plugin",
                    {"_id": plugin_info["_id"]},
                    {"$rename": {"service_type": "resource_type"}},
                )
                mongo_client.update_one(
                    "REPOSITORY",
                    "plugin",
                    {"_id": plugin_info["_id"]},
                    {"$unset": {"template": 1, "project_id": 1}},
                )


@print_log
def repository_plugin_delete_field(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "REPOSITORY",
        "plugin",
        {},
        {"$unset": {"repository_id": 1}},
    )


@print_log
def drop_collections(mongo_client):
    collections = ["repository", "policy", "schema"]
    for collection in collections:
        mongo_client.drop_collection("REPOSITORY", collection)


@print_log
def repository_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("REPOSITORY", "*")


def main(mongo_client):
    repository_drop_indexes(mongo_client)
    repository_plugin_update_drop_fields(mongo_client)
