import logging

from conf import *
from lib.util import load_yaml_from_file, check_time
from pymongo import MongoClient

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


class MongoCustomClient(object):

    def __init__(self, connection_uri: str, file_path: str = None, debug: bool = False):
        self.conn = None
        self.debug = debug
        self._create_connection_pool(connection_uri)
        if file_path:
            file_conf: dict = load_yaml_from_file(file_path)
            self.batch_size = file_conf.get('BATCH_SIZE')
            self.db_name_map = file_conf.get('DB_NAME_MAP')
            _LOGGER.debug('[Config] conf from external yaml applied')

        else:
            self.batch_size = BATCH_SIZE
            self.db_name_map = DB_NAME_MAP
            _LOGGER.debug('[Config] conf from default conf')

    @check_time
    def update_many(self, db_name: str, col_name: str, q_filter: dict, q_update: dict, q_options: dict = None):
        collection = self._get_collection(db_name, col_name)
        collection.update_many(q_filter, q_update, q_options)

    @check_time
    def update_one(self, db_name: str, col_name: str, q_filter: dict, q_update: dict, q_options: dict = None):
        collection = self._get_collection(db_name, col_name)
        collection.update_one(q_filter, q_update, q_options)

    def find(self, db_name: str, col_name: str, q_filter: dict, projection: dict = {}):
        collection = self._get_collection(db_name, col_name)
        return collection.find(q_filter, projection)

    @check_time
    def bulk_write(self, db_name: str, col_name: str, operations: list):
        collection = self._get_collection(db_name, col_name)
        total_documents_count = len(operations)
        iter_count = total_documents_count // self.batch_size
        if iter_count == 0:
            collection.bulk_write(operations)
            _LOGGER.debug(
                f'[DB-Migration] updated {len(operations)} / count : {len(operations)} / {total_documents_count}')
        else:
            updated_count = 0
            while True:
                if len(operations) <= self.batch_size:
                    updated_count += len(operations)
                    collection.bulk_write(operations)
                    _LOGGER.debug(
                        f'[DB-Migration] updated {len(operations)} / count : {updated_count} / {total_documents_count}')
                    break
                else:
                    collection.bulk_write(operations[:self.batch_size])
                    updated_count += self.batch_size
                    operations = operations[self.batch_size:]
                    _LOGGER.debug(
                        f'[DB-Migration] updated {self.batch_size} / count : {updated_count} / {total_documents_count}')

    def _create_connection_pool(self, connection_uri):
        try:
            self.conn = MongoClient(connection_uri)
            _LOGGER.debug('[Config] DB connection successful')
        except Exception as e:
            raise ValueError(f'DB Connection URI is invalid. (uri = {connection_uri})')

    def _get_collection(self, db: str, col_name: str):
        db_name = self.db_name_map.get(db)

        if db_name is None:
            raise TypeError(f'Object not constructed. Cannot access a "None" object.')

        db_names = self.conn.list_database_names()
        if db_name not in db_names:
            raise ValueError(f'Dose not found database. (db = {db_name})')

        col_names = self.conn[db_name].list_collection_names()
        if col_name not in col_names:
            raise ValueError(f'Dose not found collection. (db = {db_name}, collection = {col_name})')

        return self.conn[db_name][col_name]
