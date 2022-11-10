# DB-migration

---

# Getting started with DB-migration

## 1) Introduction

DB-migration is a tool that executes DB patch contents for MongoDB used by Cloudforet.  
Through this tool, you can easily check which DB operation occurred in a specific version.

<br>
<br>

## 2) How to use 

### 2-1) Install and Set Up DB-migration 

1) First, create a DB-migration clone in the folder you want to install (folder_path).
```shell
https://github.com/cloudforet-io/db-migration.git
```

2) After creating and activating the virtual environment, install packages used for DB-migration.  
* This guide is based on running in a virtualenv environment.

```shell
virtualenv -p 3.8 venv

source venv/bin/activate

pip3 install -r {folder_path}/pip_requirements.txt
```

### 2-2) DB-migration execution

**db-migration command**

```shell
{folder_path}/migrate.py version [-f <external_config_path>.yml] [-d]
```

- `version` : Version to use for migration (required)
- `-f {external_config_path}.yml` : external files related to config (optional)
- `-d` : Use debug mode (optional)


### 2-3) Example of DB-migration

Looking at the following example code, you can interpret it as follows:

```shell
{folder_path}/migrate.py v1.1.1
```
- It means that you are migrating v1.1.1 and don't use an external config file, use the settings in default_config.py and don't use debug mode.  
- Logs generated by DB migration are accumulated in `~/db_migration_log/v1_1_1.log`.


```shell
{folder_path}/migrate.py v1.1.1 -f {external_config_path.yml} -d
```
- Migrate v1.1.1 and use an external config file that is `external_config_path.yml`. Also, you can check the detailed debug log that occurs between DB-migration by using the debug mode.  
- Logs generated by DB migration are accumulated in `~/external_config_path/v1_1_1.log`.

<br>
<br>

## 3) Settings

```yaml
---
CONNECTION_URI: 'localhost:27017'
BATCH_SIZE: 100
LOG_PATH: '/var/log/external_log'

DB_NAME_MAP:
    # DB ALIAS: DB NAME
    IDENTITY: dev-identity
    MONITORING: dev-monitoring
    STATISTICS: dev-statistics
    SECRET: dev-secret
    REPOSITORY: dev-repository
    PLUGIN: dev-plugin
    CONFIG: dev-config
    INVENTORY: dev-inventory
```

- `CONNECTION_URI` : Connection String URI Format  
  - String URI Format follows MongoDB standard. Please refer to the following URL for more information.  
  - https://www.mongodb.com/docs/manual/reference/connection-string/#connection-string-uri-format  
- `BATCH_SIZE` : This parameter is used when using the bulk_write method.  
- `LOG_PATH` : It corresponds to the location of the log file that occurs in DB-migration.  
- `DB_NAME_MAP` : Used as a DB wrapper that maps aliases to real names. In real MongoDB, IDENTITY has the name dev-identity.

<br>
<br>

## 4) Advanced

### 4-1) folder structure

```text
├── .github
├── .gitignore
├── LICENSE
├── README.md
├── pip_requirements.txt
└── src
    ├── __init__.py
    ├── conf
    │   ├── __init__.py
    │   └── default_conf.py
    ├── lib
    │   ├── __init__.py
    │   ├── logger.py
    │   ├── mongo_custom_client.py
    │   └── util.py
    ├── migrate.py
    └── migration
        ├── __init__.py
        ├── v1_10_1.py
        ├── v1_10_2.py
        └── v1_10_3.py
```

The file structure is as follows: The parts you need to understand for dual use correspond to the files below.

* `migrate.py`   
: Execute the db-migration command

* `migration/{version}.py`  
: The details of migration for each version are specified, and you can check what kind of work was done with what collection of db actually.

* `default_conf.py`  
: If there is no external configuration file, the config specified in default_conf.py is executed.


### 4-2) Function name

Let's take a look at the detailed specification (version.py) used for the actual specific version of the db patch.

**Example of version.py**
```python
@query
def identity_service_account_set_additional_fields(mongo_client: MongoCustomClient):
    mongo_client.update_many('IDENTITY', 'service_account', {"service_account_type": {"$ne": "TRUSTED"}},
                             {"$set": {'service_account_type': 'GENERAL', 'scope': 'PROJECT'}}, upsert=True)


def main(file_path, debug):
    mongo_client: MongoCustomClient = MongoCustomClient(file_path, debug)
    identity_service_account_set_additional_fields(mongo_client)
```

- First, def main() is executed. Declare mongo_client between executions and execute the function to work with DB.

- function has `{db}_{collection}_{work content}` as the function name.

- In the above example, `identity_service_account_set_additional_fields` means a DB operation that sets additional fields in the service_account collection of the IDENTITY db.
