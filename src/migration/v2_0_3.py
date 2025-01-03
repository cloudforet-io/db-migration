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



@print_log
def file_manager_state_remove_state_project_id(mongo_client: MongoCustomClient):
    items = mongo_client.update_many("FILE-MANAGER", "file", {}, {"$unset":{"state":"", "project_id":""}})

@print_log
def file_manager_change_download_url(mongo_client: MongoCustomClient):
    file_infos = mongo_client.find("FILE-MANAGER","file",{},{})
    
    user_files = []
    files = []
    
    delete_user_files=[]

    for file_info in file_infos:
        resource_group = file.get("resource_group")
        domain_id = file.get("domain_id")
        workspace_id = file.get("workspace_id")
        user_id = file.get("user_id")
        file_id = file.get("file_id")
        
        if resource_group == "SYSTEM":
            download_url = "/files/public/" + file_id
        elif resource_group == "DOMAIN":
            if "user_id" in file_info:
                download_url = "/files/domain/" + domain_id + "/user/"+ user_id + "/" +  file_id
                file_info["download_url"] = download_url
                user_files.append(file_info)
                delete_user_files.append(file_id)
                continue
            else:
                download_url = "/files/domain/" + domain_id + "/" + file_id
        elif resource_group == "WORKSPACE":
            download_url = "/files/domain/" + domain_id + "/workspace/" + workspace_id + "/" + file_id
        elif resource_group == "PROJECT":
            download_url = "/files/domain/" + domain_id + "/workspace/" + workspace_id + "/" + file_id
        elif resource_group == "USER":
            download_url = "/files/domain/" + domain_id + "/user/"+ user_id + "/" +  file_id
            
        files.append(
            UpdateOne(
                {"_id":file_info["file"]},
                {
                    "$set": {
                        "download_url": download_url
                    }
                },
            )
        )
        
    if delete_user_files:  # 삭제할 파일 ID가 있는 경우만 실행
        mongo_client.delete_many(
            "FILE-MANAGER",  # 데이터베이스 이름
            "file",          # 컬렉션 이름
            {"file_id": {"$in": delete_user_files}}  # 조건: file_id가 리스트 안에 포함된 경우
        )
    
    if user_files:
        mongo_client.bulk_write("FILE-MANAGER", "user_file", user_files)
        
    mongo_client.bulk_write("FILE-MANAGER", "file", files)


    
    

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
    #file_manager_state_remove_state_project_id(mongo_client)
    #file_manager_change_download_url(mongo_client)
