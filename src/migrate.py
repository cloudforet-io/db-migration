#!/usr/bin/env python3
import click
import logging

from conf import DEFAULT_LOGGER
from lib import set_logger

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

_help = """
Execute DB migration based on the {version}.py file located in the migration folder.\
 Users can manage version history for DB migration.\n
Example usages:\n
    migrate.py version [-f <config_yml_path>] [-d]\n
The contents included in config yml:\n
    - BATCH_SIZE (type: int)\n
        A number of rows to be sent as a batch to the database\n
    - DB_NAME_MAP (type: dict)\n
        This is used because the database name is different depending on the environment.\n
    - LOG_PATH\n
        default: ${HOME}/db_migration_log/{version}.log
"""


@click.command(help=_help)
@click.argument('version')
@click.option('-f', '--file', 'file_path', type=click.Path(exists=True), help='Config file (YAML)', required=True)
@click.option('-d', '--debug', is_flag=True, help='Enable debug mode')
def migrate(version, file_path=None, debug=False):
    set_logger(version, file_path, debug)

    module = _get_module(version)
    getattr(module, 'main')(file_path, debug)


def _change_version_name(version: str):
    return 'v' + version.replace('.', '_')


def _get_module(version):
    changed_version = _change_version_name(version)
    return __import__(f'migration.{changed_version}', fromlist=['main'])


if __name__ == '__main__':
    migrate()
