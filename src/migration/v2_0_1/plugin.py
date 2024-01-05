import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def drop_collections(mongo_client: MongoCustomClient):
    collections = ["installed_plugin", "installed_plugin_ref", "supervisor"]
    for collection in collections:
        mongo_client.drop_collection("PLUGIN", collection)


@print_log
def plugin_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("PLUGIN", "*")
