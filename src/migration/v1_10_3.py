import logging
from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def cost_analysis_cost_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes("COST_ANALYSIS", "cost")
    mongo_client.drop_indexes("COST_ANALYSIS", "monthly_cost")
    mongo_client.drop_indexes("COST_ANALYSIS", "cost_query_history")


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v1.10.3")
    cost_analysis_cost_drop_indexes(mongo_client)
