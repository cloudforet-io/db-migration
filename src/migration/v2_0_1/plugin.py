import logging
from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

@print_log
def plugin_installed_plugin_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('PLUGIN', 'installed_plugin')


@print_log
def plugin_installed_plugin_ref_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('PLUGIN', 'installed_plugin_ref')


@print_log
def plugin_supervisor_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('PLUGIN', 'supervisor')


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, 'v2.0.0')

    plugin_installed_plugin_drop(mongo_client)
    plugin_installed_plugin_ref_drop(mongo_client)
    plugin_supervisor_drop(mongo_client)
