import logging

from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


def inventory_cloud_service_ref_refactoring(mongo_client: MongoCustomClient):
    for cloud_services_info in mongo_client.find_by_pagination(
        "INVENTORY",
        "cloud_service",
        {"cloud_service_type": "CIS-1.5"},
        {
            "workspace_id": 1,
            "ref_cloud_service_type": 1,
            "ref_region": 1,
            "provider": 1,
            "region_code": 1,
            "domain_id": 1,
        },
        show_progress=True,
    ):
        operations = []
        for cloud_service_info in cloud_services_info:
            workspace_id = cloud_service_info["workspace_id"]
            ref_cloud_service_type = cloud_service_info["ref_cloud_service_type"]
            parsed_ref_cst = ref_cloud_service_type.split(".")
            if len(parsed_ref_cst) == 5:
                continue

            ref_region = cloud_service_info["ref_region"]
            domain_id = cloud_service_info["domain_id"]
            provider = cloud_service_info["provider"]
            region_code = cloud_service_info["region_code"]

            set_params = {"$set": {}}

            if not region_code:
                ref_region = None
                parsed_ref_region = None

            else:
                if ref_region:
                    parsed_ref_region = ref_region.split(".")
                else:
                    parsed_ref_region = [domain_id, provider, region_code]

            if len(parsed_ref_cst) == 4:
                parsed_ref_cst.insert(1, workspace_id or "")
                if parsed_ref_region:
                    parsed_ref_region.insert(1, workspace_id or "")
                set_params["$set"] = {
                    "ref_cloud_service_type": ".".join(parsed_ref_cst),
                    "ref_region": ".".join(parsed_ref_region)
                    if parsed_ref_region
                    else None,
                }

                operations.append(
                    UpdateOne({"_id": cloud_service_info["_id"]}, set_params)
                )
        mongo_client.bulk_write("INVENTORY", "cloud_service", operations)


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.4")

    # cloud_service 리팩토링 작업
    inventory_cloud_service_ref_refactoring(mongo_client)

    # 추가 작업
    # Prowler의 CIS-1.5는 . 으로 분기 태우는 부분이 잘못 되어 따로 작업해야 함
