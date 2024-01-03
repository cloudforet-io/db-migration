import logging
from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def drop_collections(mongo_client):
    # drop role after refactoring role_binding
    collections = ["installed_plugin", "installed_plugin_ref", "supervisor"]
    for collection in collections:
        mongo_client.drop_collection('PLUGIN', collection)
