import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def statistics_history_drop_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection("STATISTICS", "history")


@print_log
def statistics_schedule_drop_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection("STATISTICS", "schedule")


def main(mongo_client):
    statistics_history_drop_collection(mongo_client)
    statistics_schedule_drop_collection(mongo_client)
