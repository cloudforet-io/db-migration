# A number of rows to be sent as a batch to the database
BATCH_SIZE = 1000

# This is used because the database name is different depending on the environment.
DB_NAME_MAP = {
    # DB ALIAS: DB NAME
    'IDENTITY': 'identity',
    'MONITORING': 'monitoring',
    'STATISTICS': 'statistics',
    'SECRET': 'secret',
    'REPOSITORY': 'repository',
    'PLUGIN': 'plugin',
    'CONFIG': 'config',
    'INVENTORY': 'inventory',
}

CONNECTION_URI = 'localhost:27017'

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
        'format': '{"time": "%(asctime)s.%(msecs)03dZ", "level": "%(levelname)s", "file_name": "%(filename)s", "line": %(lineno)d, "message": %(message)s}',
        'datefmt': '%Y-%m-%dT%H:%M:%S',
    }
}

LOGGER_DEFAULT_TMPL = {
    'level': 'INFO',
    'propagate': True,
    'handlers': ['console', 'file']
}