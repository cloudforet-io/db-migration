import logging

from conf import DEFAULT_LOGGER
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def drop_collections(mongo_client):
    collections = ["history", "schedule"]
    for collection in collections:
        mongo_client.drop_collection("STATISTICS", collection)
