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