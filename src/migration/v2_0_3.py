import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def dashboard_private_folder_delete_folder(mongo_client: MongoCustomClient):
    mongo_client.delete_many("DASHBOARD", "private_folder", {})


@print_log
def dashboard_private_dashboard_delete_data_table(mongo_client: MongoCustomClient):
    mongo_client.delete_many("DASHBOARD", "private_dashboard", {})


@print_log
def dashboard_private_widget_delete_widget(mongo_client: MongoCustomClient):
    mongo_client.delete_many("DASHBOARD", "private_widget", {})


@print_log
def dashboard_private_data_table_delete_data_table(mongo_client: MongoCustomClient):
    mongo_client.delete_many("DASHBOARD", "private_data_table", {})


@print_log
def dashboard_public_dashboard_delete_dashboard_without_v1(
    mongo_client: MongoCustomClient,
):
    mongo_client.delete_many(
        "DASHBOARD", "public_dashboard", {"version": {"$ne": "1.0"}}
    )


@print_log
def dashboard_public_folder_delete_folder(mongo_client: MongoCustomClient):
    mongo_client.delete_many("DASHBOARD", "public_folder", {})


@print_log
def dashboard_public_widget_delete_widget(mongo_client: MongoCustomClient):
    mongo_client.delete_many("DASHBOARD", "public_widget", {})


@print_log
def dashboard_public_dashboard_delete_data_table(mongo_client: MongoCustomClient):
    mongo_client.delete_many("DASHBOARD", "public_data_table", {})


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.3")

    # Dashboard Service
    dashboard_private_folder_delete_folder(mongo_client)
    dashboard_private_dashboard_delete_data_table(mongo_client)
    dashboard_private_widget_delete_widget(mongo_client)
    dashboard_private_data_table_delete_data_table(mongo_client)

    dashboard_public_dashboard_delete_dashboard_without_v1(mongo_client)
    dashboard_public_folder_delete_folder(mongo_client)
    dashboard_public_widget_delete_widget(mongo_client)
    dashboard_public_dashboard_delete_data_table(mongo_client)
