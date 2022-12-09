import logging
from pymongo import UpdateOne

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import query

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@query
def monitoring_alert_number_remove_collection(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('MONITORING', 'alert_number')


@query
def monitoring_alert_refactor_alert_number_by_domain_id(mongo_client: MongoCustomClient):
    domain_ids = mongo_client.distinct('MONITORING', 'alert', 'domain_id')

    records = []
    for domain_id in domain_ids:
        alerts = mongo_client.find('MONITORING', 'alert',
                                   {"domain_id": domain_id}, {"_id": 1, "created_at": 1}).sort('created_at', 1)

        if alerts:
            alert_number = 0
            operations = []
            for number, alert in enumerate(alerts, 1):
                operations.append(
                    UpdateOne({'_id': alert['_id']}, {"$set": {"alert_number": number,
                                                               "alert_number_str": str(number)}})
                )
                alert_number = number

            records.append({"domain_id": domain_id, "next": alert_number})
            mongo_client.bulk_write('MONITORING', 'alert', operations)

        _LOGGER.debug(f"alert_number changed (domain_id: {domain_id} / number: {number})")

    mongo_client.insert_many('MONITORING', 'alert_number', records, is_new=True)


def main(file_path, debug):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, debug)

    # refactor alert_number
    monitoring_alert_number_remove_collection(mongo_client)
    monitoring_alert_refactor_alert_number_by_domain_id(mongo_client)
