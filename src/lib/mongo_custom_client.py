import sys
import click
import logging
import yaml
from prompt_toolkit import prompt
from rich.console import Console
from rich.syntax import Syntax

import pymongo.collection

from conf import *
from lib.util import load_yaml_from_file, print_stage, print_finish_stage
from pymongo import MongoClient

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


class MongoCustomClient(object):

    def __init__(self, file_path: str = None, version: str = None):
        self.conn = None
        if file_path:
            self.file_conf = load_yaml_from_file(file_path)
            self.batch_size = self.file_conf.get('BATCH_SIZE', BATCH_SIZE)
            self.page_size = self.file_conf.get('PAGE_SIZE', PAGE_SIZE)
            self.db_name_map = self.file_conf.get('DB_NAME_MAP', DB_NAME_MAP)

            print_stage('SET', 'CONFIG')
            _LOGGER.debug(
                f'config from external yaml applied (file_path={file_path})')
            self._view_yaml()

        else:
            self.file_conf = None
            self.batch_size = BATCH_SIZE
            self.page_size = PAGE_SIZE
            self.db_name_map = DB_NAME_MAP
            _LOGGER.debug('conf from default conf')

        if self._ask_valid_config(version):
            self._create_connection_pool()

    def insert_many(self, db_name: str, col_name: str, records, is_new):
        _LOGGER.debug(
            f'insert_many:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- is_new: {is_new}')

        collection = self._get_collection(db_name, col_name, is_new)
        if isinstance(collection, pymongo.collection.Collection):
            collection.insert_many(records)

    def update_many(self, db_name: str, col_name: str, q_filter: dict, q_update: dict, upsert: bool = False):
        _LOGGER.debug(
            f'update_many:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- q_filter: {q_filter}\n\t'
            f'- q_update: {q_update}\n\t'
            f'- upsert: {upsert}')

        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            collection.update_many(q_filter, q_update, upsert)

    def update_one(self, db_name: str, col_name: str, q_filter: dict, q_update: dict, upsert: bool = False):
        _LOGGER.debug(
            f'update_one:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- q_filter: {q_filter}\n\t'
            f'- q_update: {q_update}\n\t'
            f'- upsert: {upsert}')

        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            collection.update_one(q_filter, q_update, upsert)

    def delete_many(self, db_name: str, col_name: str, q_filter: dict, q_options: dict = None):
        _LOGGER.debug(
            f'delete_many:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- q_filter: {q_filter}\n\t'
            f'- q_options: {q_options}')

        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            collection.delete_many(q_filter, q_options)

    def count(self, db_name: str, col_name: str, q_filter: dict):
        _LOGGER.debug(
            f'count:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- q_filter: {q_filter}')

        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.count_documents(q_filter)
        else:
            return 0

    def find(self, db_name: str, col_name: str, q_filter: dict, projection: dict = {}):
        _LOGGER.debug(
            f'find:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- q_filter: {q_filter}\n\t'
            f'- projection: {projection}')

        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.find(q_filter, projection)
        else:
            return []

    def find_by_pagination(self, db_name: str, col_name: str, q_filter: dict, projection=None, show_progress=False):
        _LOGGER.debug(
            f'find_by_pagination:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- q_filter: {q_filter}\n\t'
            f'- projection: {projection}\n\t'
            f'- show_progress: {show_progress}')

        total_count = 0

        if projection is None:
            projection = {}
        collection = self._get_collection(db_name, col_name)

        if show_progress:
            total_count = self.count(db_name, col_name, q_filter)

        if isinstance(collection, pymongo.collection.Collection):
            page_num = 0
            current_count = 0
            while True:
                skip_size = page_num * self.page_size
                cursor = collection.find(q_filter, projection).skip(skip_size).limit(self.page_size)

                items = list(cursor)
                current_count += len(items)
                if len(items) == 0:
                    if total_count != current_count:
                        count_diff = total_count - current_count
                        count_diff_str = self._create_count_diff_str(count_diff)
                        _LOGGER.error(
                            f'There is a change in the number of data. '
                            f'(expected count - actual count = {count_diff_str})')
                    break

                try:
                    current_percent = round(current_count / total_count * 100, 2)
                except ZeroDivisionError:
                    _LOGGER.debug('No data available.')
                    return []

                if show_progress:
                    _LOGGER.debug(
                        f'{db_name}.{col_name} Operated Count : {current_count}/{total_count} ({current_percent}%)')

                yield items
                page_num += 1

    def aggregate(self, db_name: str, col_name: str, pipeline: list):
        _LOGGER.debug(
            f'aggregate:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- pipeline: {pipeline}'
        )

        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.aggregate(pipeline)
        else:
            return []

    def bulk_write(self, db_name: str, col_name: str, operations: list):
        if len(operations) > 0:
            collection = self._get_collection(db_name, col_name)
            collection.bulk_write(operations)

    def get_indexes(self, db_name: str, col_name: str, comment=None):
        _LOGGER.debug(
            f'get_indexes:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- comment: {comment}'
        )

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
        _LOGGER.debug(
            f'drop_indexes:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- comment: {comment}'
        )

        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.drop_indexes(comment=comment)

    def drop_collection(self, db_name: str, col_name: str):
        _LOGGER.debug(
            f'drop_collection:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}'
        )
        collection = self._get_collection(db_name, col_name)
        if isinstance(collection, pymongo.collection.Collection):
            return collection.drop()

    def distinct(self, db_name: str, col_name: str, key: str):
        _LOGGER.debug(
            f'distinct:\n\t'
            f'- db_name: {db_name}\n\t'
            f'- col_name: {col_name}\n\t'
            f'- key: {key}'
        )
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
        _LOGGER.debug('Mongo DB connection successful')
        print_finish_stage()

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
            _LOGGER.debug(f'SKIP / {e}')
            return None

    @staticmethod
    def _create_index_key(items):
        key = {}
        for col_key, col_value in items:
            key[col_key] = col_value
        return key

    @staticmethod
    def _create_count_diff_str(count_diff):
        if count_diff == 0:
            return f'{count_diff}'
        elif count_diff > 0:
            return f'+{count_diff}'
        else:
            return f'-{abs(count_diff)}'

    def _view_yaml(self):
        yaml_str = yaml.dump(self.file_conf, allow_unicode=True, default_flow_style=False)
        syntax = Syntax(yaml_str, "yaml", theme="monokai", line_numbers=True)
        console = Console()
        console.print(syntax)

    @staticmethod
    def _ask_valid_config(version):
        while True:
            answer = prompt(
                f'The current migration version is {version}. Do you want to run with that config? (Y/N)?')

            if answer in ['Y', 'y']:
                break
            elif answer in ['N', 'n']:
                click.echo(click.style('Migration is canceled. Please check the config and try again.', fg='red'))
                sys.exit(0)
            else:
                continue

        if answer in ['Y', 'y']:
            return True
        else:
            return False
