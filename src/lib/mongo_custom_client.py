import logging
import math

import pymongo.collection

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
            self.batch_size = self.file_conf.get('BATCH_SIZE', BATCH_SIZE)
            self.page_size = self.file_conf.get('PAGE_SIZE', PAGE_SIZE)
            self.db_name_map = self.file_conf.get('DB_NAME_MAP', DB_NAME_MAP)
            _LOGGER.debug('[Config] conf from external yaml applied')

        else:
            self.file_conf = None
            self.batch_size = BATCH_SIZE
            self.page_size = PAGE_SIZE
            self.db_name_map = DB_NAME_MAP
            _LOGGER.debug('[Config] conf from default conf')
        self._create_connection_pool()

    def insert_many(self, db_name: str, col_name: str, records, is_new):
        collection = self._get_collection(db_name, col_name, is_new)
        if isinstance(collection, pymongo.collection.Collection):
            collection.insert_many(records)

    def update_many(self, db_name: str, col_name: str, q_filter: dict, q_update: dict, upsert: bool = False):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            collection.update_many(q_filter, q_update, upsert)

    def update_one(self, db_name: str, col_name: str, q_filter: dict, q_update: dict, upsert: bool = False):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            collection.update_one(q_filter, q_update, upsert)

    @check_time
    def delete_many(self, db_name: str, col_name: str, q_filter: dict, q_options: dict = None):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            collection.delete_many(q_filter, q_options)

    def count(self, db_name: str, col_name: str, q_filter: dict):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.count_documents(q_filter)
        else:
            return 0

    def find(self, db_name: str, col_name: str, q_filter: dict, projection: dict = {}):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.find(q_filter, projection)
        else:
            return []

    def find_by_pagination(self, db_name: str, col_name: str, q_filter: dict, projection=None):
        if projection is None:
            projection = {}
        collection = self._get_collection(db_name, col_name)

        if isinstance(collection, pymongo.collection.Collection):
            page_num = 0
            while True:
                skip_size = page_num * self.page_size
                cursor = collection.find(q_filter, projection).skip(skip_size).limit(self.page_size)

                items = list(cursor)
                if len(items) == 0:
                    break

                yield items
                page_num += 1

    def aggregate(self, db_name: str, col_name: str, pipeline: list):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.aggregate(pipeline)
        else:
            return []

    def bulk_write(self, db_name: str, col_name: str, operations: list):
        if len(operations) > 0:
            collection = self._get_collection(db_name, col_name)
            if isinstance(collection, pymongo.collection.Collection):
                total_operation_count = len(operations)
                batch_count = math.ceil(total_operation_count / self.batch_size)

                operated_count = 0
                for batch_num in range(batch_count):
                    start = batch_num * self.batch_size
                    end = start + self.batch_size
                    seperated_operations = operations[start:end]
                    collection.bulk_write(seperated_operations)
                    operated_count += len(seperated_operations)
        else:
            _LOGGER.debug(f'There is no operations')

    def get_indexes(self, db_name: str, col_name: str, comment=None):
        results = []
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            indexes = collection.index_information(comment=comment)

            for raw_index in indexes:
                items = indexes[raw_index]['key']

                index = {
                    'name': raw_index,
                    'v': indexes[raw_index]['v'],
                    'key': self._create_index_key(items)
                }
                results.append(index)
        return results

    def drop_indexes(self, db_name: str, col_name: str, comment=None):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.drop_indexes(comment=comment)

    def drop_collection(self, db_name: str, col_name: str):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.drop()

    def distinct(self, db_name: str, col_name: str, key: str):
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.distinct(key)

    def _create_connection_pool(self):
        if self.file_conf:
            connection_uri = self.file_conf.get('CONNECTION_URI')
        else:
            connection_uri = CONNECTION_URI

        if connection_uri is None:
            raise ValueError(f'DB Connection URI is invalid. (uri = {connection_uri})')

        self.conn = MongoClient(connection_uri, readPreference='primary')
        _LOGGER.debug('[Config] DB connection successful')

    def _get_collection(self, db: str, col_name: str, is_new: bool = False) -> [pymongo.collection.Collection, None]:
        try:
            db_name = self.db_name_map.get(db)

            if db_name is None:
                raise TypeError(f'Does not found {db} key in DB_NAME_MAP')

            db_names = self.conn.list_database_names()
            if db_name not in db_names:
                raise ValueError(f'Does not found database. (db = {db_name})')

            if not is_new:
                col_names = self.conn[db_name].list_collection_names()
                if col_name not in col_names:
                    raise ValueError(f'Dose not found collection. (db = {db_name}, collection = {col_name})')
            return self.conn[db_name][col_name]

        except Exception as e:
            _LOGGER.debug(f'[SKIP] {e}')
            return None

    @staticmethod
    def _create_index_key(items):
        key = {}
        for col_key, col_value in items:
            key[col_key] = col_value
        return key
