import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def _alert_manager_add_service_healthy_state(mongo_client: MongoCustomClient):
    # Get Service List
    for services_info in mongo_client.find_by_pagination(
        "ALERT_MANAGER", "service", {}, {"service_id": 1}
    ):
        for service_info in services_info:
            service_id = service_info["service_id"]

            is_healthy = "HEALTHY"
            unhealthy_count = mongo_client.count(
                "ALERT_MANAGER",
                "alert",
                {
                    "service_id": service_id,
                    "status": {"$in": ["TRIGGERED", "ACKNOWLEDGED"]},
                },
            )

            if unhealthy_count > 0:
                is_healthy = "UNHEALTHY"

            mongo_client.update_one(
                "ALERT_MANAGER",
                "service",
                {"service_id": service_id},
                {"$set": {"healthy_state": is_healthy}},
            )


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, "v2.0.5")

    # Add healthy_state field to service collection
    _alert_manager_add_service_healthy_state(mongo_client)
