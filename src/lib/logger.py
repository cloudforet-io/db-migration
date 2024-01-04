import sys
import logging
import logging.config
import copy
import os
import shutil
import click
from prompt_toolkit import prompt
from datetime import datetime

from conf.default_conf import *
from lib.util import load_yaml_from_file, deep_merge

__all__ = ["set_logger"]

_LOGGER = {
    "version": 1,
    "formatters": {},
    "filters": {},
    "handlers": {"console": HANDLER_DEFAULT_CONSOLE, "file": HANDLER_DEFAULT_FILE},
    "loggers": {},
}


def set_logger(version: str, file_path: str = None):
    _set_config(version, file_path)
    logging.config.dictConfig(_LOGGER)


def _set_config(version, file_path):
    global_log_conf = LOG
    external_log_dir_path = load_yaml_from_file(file_path).get("LOG_PATH", "")

    _set_default_logger(DEFAULT_LOGGER, version, external_log_dir_path)

    if "loggers" in global_log_conf:
        _set_loggers(global_log_conf["loggers"])

    if "handlers" in global_log_conf:
        _set_handlers(global_log_conf["handlers"])

    if "formatters" in global_log_conf:
        _set_formatters(global_log_conf["formatters"])


def _set_default_logger(default_logger, version, external_log_dir_path):
    _LOGGER["loggers"] = {default_logger: LOGGER_DEFAULT_TMPL}
    _LOGGER["formatters"] = FORMATTER_DEFAULT_TMPL

    _set_log_file_path(version, external_log_dir_path)


def _set_log_file_path(version, external_log_dir_path):
    home = os.path.expanduser("~")

    file_path = f"{home}/{LOG_PATH}/{version}.log"
    if not external_log_dir_path:
        if not os.path.isdir(os.path.join(home, LOG_PATH)):
            os.mkdir(os.path.join(home, LOG_PATH))
            os.mkdir(os.path.join(home, LOG_PATH, "backup"))
    else:
        file_path = _set_external_file_path(external_log_dir_path, version)

    _LOGGER["handlers"]["file"]["filename"] = file_path


def _set_external_file_path(external_file_path, version):
    file_path = f"{external_file_path}/{version}.log"

    if os.path.exists(file_path):
        logs = [log for log in os.listdir(external_file_path) if version in log]
        target_log = sorted(logs)[-1]

        _check_duplicated_migration(external_file_path, target_log)

    if not os.path.isdir(external_file_path):
        os.mkdir(external_file_path)
        os.mkdir(os.path.join(external_file_path, "backup"))

    return file_path


def _set_loggers(loggers):
    for _logger in loggers:
        _LOGGER["loggers"][_logger] = deep_merge(
            loggers[_logger], copy.deepcopy(LOGGER_DEFAULT_TMPL)
        )


def _set_handlers(handlers):
    for _handler in handlers:
        _default = copy.deepcopy(HANDLER_DEFAULT_TMPL)

        if "type" in handlers[_handler]:
            if handlers[_handler]["type"] not in HANDLER_DEFAULT_TMPL:
                raise TypeError("Logger handler type is not supported")

            _default = copy.deepcopy(HANDLER_DEFAULT_TMPL[handlers[_handler]["type"]])

        _default = deep_merge(handlers[_handler], _default)

        if "type" in _default:
            del _default["type"]

        _LOGGER["handlers"][_handler] = _default


def _set_formatters(formatters):
    for _formatter in formatters:
        _default = {}

        if "type" in formatters[_formatter]:
            if formatters[_formatter]["type"] not in FORMATTER_DEFAULT_TMPL:
                raise TypeError("Logger formatter type is not supported")

            _default = copy.deepcopy(
                FORMATTER_DEFAULT_TMPL[formatters[_formatter]["type"]]
            )

        _default = deep_merge(formatters[_formatter]["args"], _default)

        if "type" in _default:
            del _default["type"]

        _LOGGER["formatters"][_formatter] = _default


def _check_duplicated_migration(external_file_path, file_path):
    while True:
        answer = prompt(
            f"Previous db-migration history exists. Would you like to migrate?({file_path}) (Y/N)? : "
        )

        if answer in ["Y", "y"]:
            today = datetime.today().strftime("%Y%m%d.%H%m%S")
            version, subfix = file_path.split(".log")
            saved_file_name = f"{version}.{today}.log"
            old_path = os.path.join(external_file_path, file_path)
            new_path = os.path.join(external_file_path, "backup", saved_file_name)
            shutil.copyfile(old_path, new_path)

            click.echo(f"The old log file has been saved. (saved file path={new_path})")
            break

        elif answer in ["N", "n"]:
            click.echo(click.style("Migration is canceled.", fg="red"))
            sys.exit(0)

        else:
            continue
