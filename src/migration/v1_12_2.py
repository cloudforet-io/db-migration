import logging
from conf import DEFAULT_LOGGER
from lib import MongoCustomClient
from lib.util import print_log
from pymongo import UpdateOne

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


# cost-analysis
@print_log
def cost_analysis_public_dashboard_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'public_dashboard')


@print_log
def cost_analysis_user_dashboard_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'user_dashboard')


@print_log
def cost_analysis_custom_widget_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'custom_widget')


@print_log
def cost_analysis_cost_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'cost')


@print_log
def cost_analysis_monthly_cost_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'monthly_cost')


@print_log
def cost_analysis_budget_usage_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'budget_usage')


@print_log
def cost_analysis_cost_query_set_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'cost_query_set')


@print_log
def cost_analysis_cost_query_history_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'cost_query_history')


@print_log
def cost_analysis_budget_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'budget')


@print_log
def cost_analysis_job_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'job')


@print_log
def cost_analysis_job_task_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('COST-ANALYSIS', 'job_task')


# inventory
@print_log
def inventory_cloud_service_stats_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'cloud_service_stats')


@print_log
def inventory_monthly_cloud_service_stats_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'monthly_cloud_service_stats')


@print_log
def inventory_cloud_service_query_sets_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'cloud_service_query_sets')


@print_log
def inventory_cloud_service_stats_query_history_drop(mongo_client: MongoCustomClient):
    mongo_client.drop_collection('INVENTORY', 'cloud_service_stats_query_history')


@print_log
def inventory_prowler_change_options_to_compliance_framework(mongo_client: MongoCustomClient):
    prowler_filter = {
        'plugin_info.plugin_id': 'plugin-prowler-inven-collector',
        'provider': 'aws'
    }

    prowler_records = mongo_client.find('INVENTORY', 'collector', prowler_filter, {'_id': 1, 'plugin_info': 1})

    operations = []
    for record in prowler_records:
        if record['plugin_info']['options'].get('compliance_type'):
            operations.append(
                UpdateOne(
                    {'_id': record['_id']},
                    {"$rename": {'plugin_info.options.compliance_type': 'plugin_info.options.compliance_framework'}}
                )
            )

    mongo_client.bulk_write('INVENTORY', 'collector', operations)


def main(file_path):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, 'v1.12.2')

    # cost-analysis
    cost_analysis_public_dashboard_drop(mongo_client)
    cost_analysis_user_dashboard_drop(mongo_client)
    cost_analysis_custom_widget_drop(mongo_client)
    cost_analysis_cost_drop(mongo_client)
    cost_analysis_monthly_cost_drop(mongo_client)
    cost_analysis_budget_usage_drop(mongo_client)
    cost_analysis_cost_query_set_drop(mongo_client)
    cost_analysis_cost_query_history_drop(mongo_client)
    cost_analysis_budget_drop(mongo_client)
    cost_analysis_job_drop(mongo_client)
    cost_analysis_job_task_drop(mongo_client)

    # inventory
    inventory_cloud_service_stats_drop(mongo_client)
    inventory_monthly_cloud_service_stats_drop(mongo_client)
    inventory_cloud_service_query_sets_drop(mongo_client)
    inventory_cloud_service_stats_query_history_drop(mongo_client)
    inventory_prowler_change_options_to_compliance_framework(mongo_client)
