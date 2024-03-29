import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def drop_collections(mongo_client: MongoCustomClient):
    collections = ["board", "post"]
    for collection in collections:
        mongo_client.drop_collection("BOARD", collection)


@print_log
def board_post_update_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "BOARD",
        "post",
        {},
        {"$rename": {"board_id": "board_type", "scope": "resource_group"}},
    )

    mongo_client.update_many("BOARD", "post", {}, {"$set": {"board_type": "NOTICE"}})

    mongo_client.update_many(
        "BOARD",
        "post",
        {"resource_group": "PUBLIC"},
        {"$set": {"resource_group": "SYSTEM"}},
    )


@print_log
def board_post_drop_fields(mongo_client: MongoCustomClient):
    params = {
        "$unset": {
            "post_type": 1,
            "user_domain_id": 1,
        }
    }
    mongo_client.update_many("BOARD", "post", {}, params)


@print_log
def board_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("BOARD", "*")


def main(mongo_client):
    board_drop_indexes(mongo_client)
    board_post_update_fields(mongo_client)
    board_post_drop_fields(mongo_client)
