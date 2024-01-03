import logging

from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log
from pymongo import UpdateOne

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


@print_log
def cost_analysis_data_source_and_data_source_rule_refactoring(mongo_client, domain_id_param):
    # TODO : domain.tags에 따라 EA 계약인 경우 resource_group=WORKSPACE일 수 있다. 이 부분 추가 필요
    set_param = {
        '$set': {
            'resource_group': 'DOMAIN'
            , 'workspace_id': '*'
        }
    }
    mongo_client.update_many('COST_ANALYSIS', 'data_source', {}, set_param)


@print_log
def cost_analysis_budget_and_budget_usage_refactoring(mongo_client, domain_id_param, workspace_map, project_map, workspace_mode):
    resource_group = ''
    workspace_id = ''

    budget_infos = mongo_client.find('COST_ANALYSIS', 'budget', {'domain_id':domain_id_param}, {})
    for budget_info in budget_infos: 
        # For idempotent
        if budget_info.get('workspace_id'):
            continue

        # if project group id exists, resource_group = WORKSPACE, or PROJECT
        if not budget_info.get('project_group_id'):
            resource_group = 'PROJECT'
            workspace_id = project_map[budget_info['domain_id']].get(budget_info.get('project_id', '-'))
        else:
            # never happen
            if not budget_info.get('project_id'):
                _LOGGER.error(f"Budget({budget_info['budget_id']}) has no project and project_group_id. (domain_id: {domain_id_param})")

            resource_group = 'WORKSPACE'
            if workspace_mode: 
                workspace_id = workspace_map['multi'][budget_info['domain_id']].get(budget_info.get('project_group_id'))
            else:
                workspace_id = workspace_map['single'][budget_info['domain_id']]

        set_params = {
            '$set': {
                'resource_group': resource_group
                , 'workspace_id': workspace_id
            }, '$unset': {
                'project_group_id': 1
            }
        }

        mongo_client.update_one('COST_ANALYSIS', 'budget', {'_id': budget_info['_id']}, {set_params})

        # max under 20. so update_many.
        mongo_client.update_many('COST_ANALYSIS', 'budget_usage', {'budget_id':budget_info['budget_id']}, {set_params})


@print_log
def cost_analysis_cost_refactoring(mongo_client, domain_id_param, workspace_map, project_map, workspace_mode):
    workspace_id = ''
    operations = []
    item_count = 0
    for costs_info in mongo_client.find_by_pagination('COST_ANALYSIS', 'cost', {'domain_id':domain_id_param}, {'_id': 1}):
        operations = []
        for cost_info in costs_info:
            # For idempotent
            if cost_info.get('workspace_id'):
                continue

            if cost_info.get('project_id'):
                # if project_id is not null
                workspace_id = project_map[cost_info['domain_id']].get(cost_info.get('project_id'))
            elif cost_info.get('project_group_id'):
                # if project_id is null, project_group_id is not null
                if workspace_mode: 
                    workspace_id = workspace_map['multi'][cost_info['domain_id']].get(cost_info.get('project_group_id'))
                else:
                    workspace_id = workspace_map['single'][cost_info['domain_id']]
            else:
                # if not exists both project_id, project_group_id
                # if project_id is null, project_group_id is not null
                if workspace_mode: 
                    workspace_id = None
                else:
                    workspace_id = workspace_map['single'][cost_info['domain_id']]
                
            set_params = {
                '$set': {
                    'workspace_id': workspace_id
                }, '$unset': {
                    'project_group_id': 1
                }
            }
            
            operations.append(
                UpdateOne({'_id': cost_info['_id']}, set_params)
            )

            item_count += 1
        
        mongo_client.bulk_write('COST_ANALYSIS', 'cost', operations)
        _LOGGER.info(f'Total Count : {item_count}')


@print_log
def drop_collections(mongo_client):
    # drop role after refactoring role_binding
    collections = ["job", "job_task"]
    for collection in collections:
        mongo_client.drop_collection('COST_ANALYSIS', collection)


def main(mongo_client, domain_id_param, workspace_map, project_map, workspace_mode):
    # data_source, data_source_rule
    cost_analysis_data_source_and_data_source_rule_refactoring(mongo_client, domain_id_param)

    # budget, budget_usage
    cost_analysis_budget_and_budget_usage_refactoring(mongo_client, domain_id_param, workspace_map, project_map, workspace_mode)

    # cost
    cost_analysis_cost_refactoring(mongo_client, domain_id_param, workspace_map, project_map. workspace_mode)
