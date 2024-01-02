import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log
from pymongo import UpdateOne

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

@print_log
def notification_project_channel_refactoring(mongo_client: MongoCustomClient, domain_id_param, project_map):
    workspace_id = ''
    operations = []

    project_channel_infos = mongo_client.find('NOTIFICATION', 'project_channel', {'domain_id':domain_id_param}, {})
    for project_channel_info in project_channel_infos:
        workspace_id = project_map[project_channel_info['domain_id']].get(project_channel_info['project_id'])
        operations.append(
            UpdateOne(
                {'_id': project_channel_info['_id']},
                {"$set": {'workspace_id': workspace_id}}
            )
        )
    
    mongo_client.bulk_write('NOTIFICATION', 'project_channel', operations)


def main(mongo_client, domain_id_param, project_map):
    notification_project_channel_refactoring(mongo_client, domain_id_param, project_map)

