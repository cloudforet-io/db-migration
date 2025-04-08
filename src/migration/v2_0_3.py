import logging
import html

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



@print_log
def file_manager_record_delete_state_download_url_file_type_(mongo_client: MongoCustomClient):
    items = mongo_client.update_many("FILE-MANAGER", "file", {}, {"$unset":{"state":"","file_type":"","download_url":""}})
    _LOGGER.info(f"Total file-manager delete Count : {items}")


@print_log
def board_add_contents_type(mongo_client: MongoCustomClient):
    mongo_client.update_many("BOARD", "board", {}, {"$set": {"contents_type": "html"}})
    
@print_log
def board_change_contents(mongo_client: MongoCustomClient):
    board_contents_list = mongo_client.find("BOARD","board",{},{})

    update_contents = []

    for contents in board_contents_list:
        contents["contents"] = html.escape(contents["contents"])
        update_contents.append(contents)

    if update_contents:
        mongo_client.bulk_write("BOARD", "board", update_contents)
    
    

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
    
    # File Manager Service
    file_manager_record_delete_state_download_url_file_type_(mongo_client)
    board_add_contents_type(mongo_client)
    board_change_contents(mongo_client)
    mongo_client.close()
    
