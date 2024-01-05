import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def file_update_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many(
        "FILE_MANAGER",
        "file",
        {},
        {
            "$rename": {"scope": "resource_group"},
            "$set": {"workspace_id": "*"},
            "$unset": {"user_domain_id": 1},
        },
    )

    mongo_client.update_many(
        "FILE_MANAGER",
        "file",
        {"resource_group": "PUBLIC"},
        {"$set": {"resource_group": "SYSTEM"}},
    )


@print_log
def file_manager_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("FILE_MANAGER", "*")


@print_log
def file_delete_documents(mongo_client: MongoCustomClient):
    file_manager_drop_indexes(mongo_client)
    mongo_client.delete_many(
        "FILE_MANAGER",
        "file",
        {"file_type": "xlsx"},
    )
