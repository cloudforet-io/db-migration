import logging

import yaml
import re
import functools
import time
import click
import shutil

from conf import DEFAULT_LOGGER

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

TERMINAL_WIDTH = shutil.get_terminal_size(fallback=(120, 50)).columns
YAML_LOADER = yaml.Loader
YAML_LOADER.add_implicit_resolver(
    u'tag:yaml.org,2002:float',
    re.compile(u'''^(?:
     [-+]?(?:[0-9][0-9_]*)\\.[0-9_]*(?:[eE][-+]?[0-9]+)?
    |[-+]?(?:[0-9][0-9_]*)(?:[eE][-+]?[0-9]+)
    |\\.[0-9_]+(?:[eE][-+][0-9]+)?
    |[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\\.[0-9_]*
    |[-+]?\\.(?:inf|Inf|INF)
    |\\.(?:nan|NaN|NAN))$''', re.X),
    list(u'-+0123456789.'))


def load_yaml(yaml_str: str) -> dict:
    try:
        return yaml.load(yaml_str, Loader=YAML_LOADER)
    except Exception:
        raise ValueError(f'YAML Load Error: {yaml_str}')


def load_yaml_from_file(yaml_file: str) -> dict:
    try:
        with open(yaml_file, 'r') as f:
            return load_yaml(f)
    except Exception:
        raise Exception(f'YAML Load Error: {yaml_file}')


def print_stage(action, name):
    click.echo(f' [{action}] {name} '[:TERMINAL_WIDTH].center(TERMINAL_WIDTH, '='))


def print_finish_stage(action=None, name=None):
    if action and name:
        click.echo(f' [{action}] {name} '[:TERMINAL_WIDTH].center(TERMINAL_WIDTH, '='))
    else:
        click.echo(f''[:TERMINAL_WIDTH].center(TERMINAL_WIDTH, '='))
    click.echo('')


def query(func):
    @functools.wraps(func)
    def newFunc(*args, **kwargs):
        print_stage('EXECUTE', func.__name__)
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        _LOGGER.debug(f'[Total time] {seconds_to_human_readable(int(end - start))}')
        print_finish_stage('DONE', func.__name__)

    return newFunc


def deep_merge(from_dict: dict, into_dict: dict) -> dict:
    for key, value in from_dict.items():
        if isinstance(value, dict):
            node = into_dict.setdefault(key, {})
            deep_merge(value, node)
        else:
            into_dict[key] = value

    return into_dict


def seconds_to_human_readable(seconds: int) -> str:
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return "%02d:%02d:%02d" % (hours, minutes, seconds)
