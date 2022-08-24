LOG = {}

DEFAULT_LOGGER = 'migration'

HANDLER_DEFAULT_CONSOLE = {
    'class': 'logging.StreamHandler',
    'formatter': 'standard',
    'filters': []
}

HANDLER_DEFAULT_FILE = {
    'class': 'logging.handlers.RotatingFileHandler',
    'filename': '',
    'filters': [],
    'formatter': 'file',
    'maxBytes': 10485760,  # 10 MB
    'backupCount': 10
}

HANDLER_DEFAULT_TMPL = {
    'console': HANDLER_DEFAULT_CONSOLE,
    'file': HANDLER_DEFAULT_FILE
}

FORMATTER_DEFAULT_TMPL = {
    'standard': {
        'format': '%(asctime)s.%(msecs)03dZ [%(levelname)s] %(message)s',
        'datefmt': '%Y-%m-%dT%H:%M:%S',
    },
    'file': {
        'format': '{"time": "%(asctime)s.%(msecs)03dZ", "level": "%(levelname)s", "file_name": "%(filename)s", "line": %(lineno)d, "message": %(msg_dump)s}',
        'datefmt': '%Y-%m-%dT%H:%M:%S',
    }
}

_LOGGER = {
    'version': 1,
    'formatters': {},
    'filters': {},
    'handlers': {
        'console': HANDLER_DEFAULT_CONSOLE
    },
    'loggers': {}
}

LOGGER_DEFAULT_TMPL = {
    'level': 'INFO',
    'propagate': True,
    'handlers': ['console']
}
