import logging
from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def inventory_record_delete_project_id(mongo_client: MongoCustomClient):
    item_count = 0
    for items in mongo_client.find_by_pagination("INVENTORY", "record", {}, {"_id": 1}):
        query_filter = {"_id": {"$in": []}}
        for item in items:
            query_filter["_id"]["$in"].append(item["_id"])
            item_count += 1

        mongo_client.update_many(
            "INVENTORY",
            "record",
            query_filter,
            {"$unset": {"project_id": ""}},
            upsert=True,
        )
        _LOGGER.info(f"Total Count : {item_count}")


@print_log
def inventory_cloud_service_tag_delete_project_id(mongo_client: MongoCustomClient):
    item_count = 0
    for items in mongo_client.find_by_pagination(
        "INVENTORY", "cloud_service_tag", {}, {"_id": 1}
    ):
        query_filter = {"_id": {"$in": []}}
        for item in items:
            query_filter["_id"]["$in"].append(item["_id"])
            item_count += 1

        mongo_client.update_many(
            "INVENTORY",
            "cloud_service_tag",
            query_filter,
            {"$unset": {"project_id": ""}},
            upsert=True,
        )
        _LOGGER.info(f"Total Count : {item_count}")


@print_log
def inventory_cloud_service_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("INVENTORY", "cloud_service")


@print_log
def inventory_cloud_service_tag_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("INVENTORY", "cloud_service_tag")


@print_log
def inventory_collection_state_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("INVENTORY", "collection_state")


@print_log
def inventory_record_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("INVENTORY", "record")


@print_log
def inventory_cloud_service_type_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("INVENTORY", "cloud_service_type")


@print_log
def inventory_region_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("INVENTORY", "region")


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v1.10.4")

    inventory_record_delete_project_id(mongo_client)
    inventory_cloud_service_tag_delete_project_id(mongo_client)

    inventory_cloud_service_drop_indexes(mongo_client)
    inventory_cloud_service_tag_drop_indexes(mongo_client)
    inventory_collection_state_drop_indexes(mongo_client)
    inventory_record_drop_indexes(mongo_client)
    inventory_cloud_service_type_drop_indexes(mongo_client)
    inventory_region_drop_indexes(mongo_client)
