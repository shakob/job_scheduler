#!/usr/bin/python3

import sys
from enum import Enum
from functools import partial, update_wrapper
from threading import Lock

import logbook

__all__ = ['Logger', 'id_counter', 'JobStatusEnum', 'JobTypeEnum',
           'TimeUnitEnum', 'ThreadSafeCounter']


class _Logger:
    _FORMAT_STRING = '[{record.time}] {record.level_name:>7s}: ' \
                     '{record.thread_name:13s} {record.message:s}'

    def __init__(self):
        self._handler = logbook.StreamHandler(
            sys.stdout, level="INFO", format_string=_Logger._FORMAT_STRING,
            encoding="utf-8", filter=None, bubble=True)
        self._handler.push_application()
        self._logger = logbook.Logger('Scheduler')

    def info(self, msg):
        return self._logger.info(msg)

    def warning(self, msg):
        return self._logger.warning(msg)


# Singleton
Logger = _Logger()


class ThreadSafeCounter:

    def __init__(self):
        self._lock = Lock()
        self._i = 0

    def __iter__(self):
        return self

    @property
    def value(self):
        with self._lock:
            return self._i

    def next(self):
        with self._lock:
            self._i += 1
            return self._i


class JobStatusEnum(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TERMINATED = "TERMINATED"


class TimeUnitEnum(Enum):
    HOURS = "hours"
    SECONDS = "seconds"  # Testing


class JobTypeEnum(Enum):
    BACKUP = "Backup"
    BUILD = "Build"
    REGRESSION = "Regression"


def get_partial_function(func, *args, **kwargs):
    """
    job_funcs already wrapped by partial won't have
    __name__, __module__ or __doc__ and the update_wrapper()
    call will fail.

    :param func: Function to call
    :param args: args
    :param kwargs: kwargs
    :return: Wrapped partial function ready for execution
    :rtype: Callable
    """
    job_func = partial(func, *args, **kwargs)
    try:
        update_wrapper(job_func, func)
    except AttributeError:
        pass
    return job_func
