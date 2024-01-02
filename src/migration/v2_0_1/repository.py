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
def repository_policy_drop_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection("REPOSITORY", "policy")


@print_log
def repository_repository_drop_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection("REPOSITORY", "repository")


@print_log
def repository_schema_drop_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection("REPOSITORY", "schema")


def main(mongo_client):
    repository_plugin_update_drop_fields(mongo_client)
    repository_policy_drop_collection(mongo_client)
    repository_repository_drop_collection(mongo_client)
    repository_schema_drop_collection(mongo_client)
