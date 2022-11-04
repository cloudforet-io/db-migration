import logging
from conf import DEFAULT_LOGGER
from pymongo import UpdateOne, DeleteOne
from lib import MongoCustomClient
from lib.util import query

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@query
def cost_analysis_cost_drop_indexes(mongo_client: MongoCustomClient):
    mongo_client.drop_indexes('COST_ANALYSIS', 'cost')
    mongo_client.drop_indexes('COST_ANALYSIS', 'monthly_cost')
    mongo_client.drop_indexes('COST_ANALYSIS', 'cost_query_history')


def main(file_path, debug):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, debug)
    cost_analysis_cost_drop_indexes(mongo_client)
