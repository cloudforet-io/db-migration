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
            repository_item = mongo_client.find(
                "REPOSITORY",
                "repository",
                {
                    "repository_id": plugin_info["repository_id"],
                    "repository_type": "local",
                },
                {},
            )
            if repository_item:
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
def drop_collections(mongo_client):
    # drop role after refactoring role_binding
    collections = ["policy", "repository", "schema"]
    for collection in collections:
        mongo_client.drop_collection("REPOSITORY", collection)


def main(mongo_client):
    repository_plugin_update_drop_fields(mongo_client)
