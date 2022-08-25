import logging
import logging.config
import copy

from conf.default_conf import *
from lib.util import deep_merge

__all__ = ['set_logger']

_LOGGER = {
    'version': 1,
    'formatters': {},
    'filters': {},
    'handlers': {
        'console': HANDLER_DEFAULT_CONSOLE
    },
    'loggers': {}
}


def set_logger(debug: bool = False):
    _set_config(debug)
    logging.config.dictConfig(_LOGGER)


def _set_config(debug):
    global_log_conf = LOG

    _set_default_logger(DEFAULT_LOGGER, debug)

    if 'loggers' in global_log_conf:
        _set_loggers(global_log_conf['loggers'])

    if 'handlers' in global_log_conf:
        _set_handlers(global_log_conf['handlers'])

    if 'formatters' in global_log_conf:
        _set_formatters(global_log_conf['formatters'])


def _set_default_logger(default_logger, debug):
    _LOGGER['loggers'] = {default_logger: LOGGER_DEFAULT_TMPL}
    _LOGGER['formatters'] = FORMATTER_DEFAULT_TMPL

    if debug:
        _LOGGER['loggers'][DEFAULT_LOGGER]['level'] = 'DEBUG'


def _set_loggers(loggers):
    for _logger in loggers:
        _LOGGER['loggers'][_logger] = deep_merge(loggers[_logger], copy.deepcopy(LOGGER_DEFAULT_TMPL))


def _set_handlers(handlers):
    for _handler in handlers:
        _default = copy.deepcopy(HANDLER_DEFAULT_TMPL)

        if 'type' in handlers[_handler]:
            if handlers[_handler]['type'] not in HANDLER_DEFAULT_TMPL:
                raise TypeError('Logger handler type is not supported')

            _default = copy.deepcopy(HANDLER_DEFAULT_TMPL[handlers[_handler]['type']])

        _default = deep_merge(handlers[_handler], _default)

        if 'type' in _default:
            del _default['type']

        _LOGGER['handlers'][_handler] = _default


def _set_formatters(formatters):
    for _formatter in formatters:
        _default = {}

        if 'type' in formatters[_formatter]:
            if formatters[_formatter]['type'] not in FORMATTER_DEFAULT_TMPL:
                raise TypeError('Logger formatter type is not supported')

            _default = copy.deepcopy(FORMATTER_DEFAULT_TMPL[formatters[_formatter]['type']])

        _default = deep_merge(formatters[_formatter]['args'], _default)

        if 'type' in _default:
            del _default['type']

        _LOGGER['formatters'][_formatter] = _default
