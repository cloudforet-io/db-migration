def main(mongo_client, project_map):
    repository_plugin_update_drop_fields(mongo_client)
    repository_policy_drop_collection(mongo_client)
    repository_repository_drop_collection(mongo_client)
    repository_schema_drop_collection(mongo_client)
