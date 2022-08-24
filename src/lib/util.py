import logging

import yaml
import re
import functools
import time

from conf import DEFAULT_LOGGER

_LOGGER = logging.getLogger(DEFAULT_LOGGER)

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


def check_time(func):
    @functools.wraps(func)
    def newFunc(*args, **kwargs):
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        _LOGGER.debug(f'[Total time] {end - start:.8f} sec')

    return newFunc


def query(func):
    @functools.wraps(func)
    def newFunc(*args, **kwargs):
        _LOGGER.info(f'[EXECUTE] {func.__name__} >>>>>>>>')
        func(*args, **kwargs)
        _LOGGER.info(f'[DONE] {func.__name__} >>>>>>>>\n\n')

    return newFunc


def deep_merge(from_dict: dict, into_dict: dict) -> dict:
    for key, value in from_dict.items():
        if isinstance(value, dict):
            node = into_dict.setdefault(key, {})
            deep_merge(value, node)
        else:
            into_dict[key] = value

    return into_dict
