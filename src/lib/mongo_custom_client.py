import copy
import logging

from conf import *
from lib.util import load_yaml_from_file, check_time
from pymongo import MongoClient

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


class MongoCustomClient(object):

    def __init__(self, file_path: str = None, debug: bool = False):
        self.conn = None
        self.debug = debug
        if file_path:
            self.file_conf = load_yaml_from_file(file_path)
            self.batch_size = self.file_conf.get('BATCH_SIZE')
            self.db_name_map = self.file_conf.get('DB_NAME_MAP')
            _LOGGER.debug('[Config] conf from external yaml applied')

        else:
            self.file_conf = None
            self.batch_size = BATCH_SIZE
            self.db_name_map = DB_NAME_MAP
            _LOGGER.debug('[Config] conf from default conf')
        self._create_connection_pool()

    @check_time
    def update_many(self, db_name: str, col_name: str, q_filter: dict, q_update: dict, q_options: dict = None):
        collection = self._get_collection(db_name, col_name)
        collection.update_many(q_filter, q_update, q_options)

    @check_time
    def update_one(self, db_name: str, col_name: str, q_filter: dict, q_update: dict, q_options: dict = None):
        collection = self._get_collection(db_name, col_name)
        collection.update_one(q_filter, q_update, q_options)

    @check_time
    def delete_many(self, db_name: str, col_name: str, q_filter: dict, q_options: dict = None):
        collection = self._get_collection(db_name, col_name)
        collection.delete_many(q_filter, q_options)

    def find(self, db_name: str, col_name: str, q_filter: dict, projection: dict = {}):
        collection = self._get_collection(db_name, col_name)
        return collection.find(q_filter, projection)

    @check_time
    def bulk_write(self, db_name: str, col_name: str, operations: list):
        if len(operations) > 0:
            collection = self._get_collection(db_name, col_name)
            total_operations_count = len(operations)
            iter_count = (total_operations_count // self.batch_size) + 1

            updated_count = 0
            for operated_count in range(iter_count):
                if len(operations) <= self.batch_size:
                    collection.bulk_write(operations)
                    updated_count += len(operations)
                    _LOGGER.debug(
                        f'[DB-Migration] Operated {len(operations)} / count : {updated_count} / {total_operations_count}')
                else:
                    collection.bulk_write(operations[:self.batch_size])
                    operations = operations[self.batch_size:]
                    updated_count += self.batch_size
                    _LOGGER.debug(
                        f'[DB-Migration] Operated {self.batch_size} / count : {updated_count} / {total_operations_count}')
        else:
            _LOGGER.debug(f'There is no operations')

    def _create_connection_pool(self):
        if self.file_conf:
            connection_uri = self.file_conf.get('CONNECTION_URI')
        else:
            connection_uri = CONNECTION_URI

        if connection_uri is None:
            raise ValueError(f'DB Connection URI is invalid. (uri = {connection_uri})')

        self.conn = MongoClient(connection_uri)
        _LOGGER.debug('[Config] DB connection successful')

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
