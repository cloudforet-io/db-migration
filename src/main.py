#!/usr/bin/env python3
import os
import click


def _change_version_name(version: str):
    return 'v' + version.replace('.', '_')


def _get_module(version):
    changed_version = _change_version_name(version)
    return __import__(f'migration.{changed_version}', fromlist=['main'])


@click.command()
@click.argument('version')
@click.option('-c', '--connection-uri', help='MongoDB Connection URI', type=str, show_default=True,
              default=lambda: os.environ.get('DB_CONNECTION_URI'))
@click.option('-f', '--file-parameter', 'file_path', type=click.Path(exists=True), help='Config file (YAML)')
@click.option('-d', '--debug', help='Enable debug mode')
def migrate(version, connection_uri, file_path=None, debug=False):
    """Execute DB migration and manage version as code."""
    if connection_uri is None:
        raise ValueError(f'connection_uri is invalid. (connection_uri={connection_uri})')
    module = _get_module(version)
    getattr(module, 'main')(connection_uri, file_path, debug)


if __name__ == '__main__':
    migrate()
