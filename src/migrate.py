#!/usr/bin/env python3
import click
import logging

from conf import DEFAULT_LOGGER
from lib import set_logger

_LOGGER = logging.getLogger(DEFAULT_LOGGER)


def _change_version_name(version: str):
    return 'v' + version.replace('.', '_')


def _get_module(version):
    changed_version = _change_version_name(version)
    return __import__(f'migration.{changed_version}', fromlist=['main'])


@click.command()
@click.argument('version')
@click.option('-f', '--file-parameter', 'file_path', type=click.Path(exists=True), help='Config file (YAML)')
@click.option('-d', '--debug', help='Enable debug mode')
def migrate(version, file_path=None, debug=False):
    """Execute DB migration and manage version as code."""
    set_logger(debug)

    module = _get_module(version)
    getattr(module, 'main')(file_path, debug)


if __name__ == '__main__':
    migrate()
