import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def drop_collections(mongo_client):
    collections = ["repository", "policy", "schema", "plugin"]
    for collection in collections:
        mongo_client.drop_collection("REPOSITORY", collection)
