#!/usr/bin/python3
import traceback
from abc import ABCMeta
from datetime import datetime, timedelta
from threading import Event, Lock
from typing import Dict

from scheduler import ThreadSafeCounter, JobTypeEnum, TimeUnitEnum, \
    get_partial_function, JobStatusEnum, Logger

__all__ = ["Job", "JOB_FACTORY"]

"""
Available periodic times.
- 0 means run once
- All others is the interval depends on the time unit
"""
AVAILABLE_PERIODIC = [0, 1, 3, 6, 12]


class Job(metaclass=ABCMeta):
    """
    Job Class
    """

    """
    Job ID counter - thread safe for giving ID for each Job
    """
    JOB_ID_COUNTER = ThreadSafeCounter()

    def __init__(self, job_type: JobTypeEnum, interval: int,
                 time_unit: TimeUnitEnum, *args, **kwargs):
        assert interval in AVAILABLE_PERIODIC, \
            "interval should be one of the following: '{}'" \
                .format(AVAILABLE_PERIODIC)
        assert isinstance(time_unit, TimeUnitEnum), \
            "time_unit should be of type: '{TimeUnitEnum}'"

        self.job_id = Job.JOB_ID_COUNTER.next()

        self._job_func = get_partial_function(self._do_job, *args, **kwargs)
        self._job_type = job_type
        self._unit_enum = time_unit
        self._interval = interval
        self._is_repeatable = self._interval != 0
        self.status = JobStatusEnum.PENDING

        self._mark_for_abortion = Event()
        self._abort_lock = Lock()

        self._iterations = 0
        self._start_time = datetime.now()
        self._period = timedelta(**{self._unit_enum.value: self._interval})

        self.next_run = None
        self._last_run_started = None
        self._last_run_ended = None

        # Stores last execution result
        self._last_result = None

        self._schedule_next_run()

    @property
    def last_result(self):
        """
        :return: Last job execution result
        :rtype: Any
        """
        return self._last_result

    def __eq__(self, other: 'Job'):
        if isinstance(other, Job):
            return self.job_id == other.job_id
        return False

    def __lt__(self, other: 'Job'):
        if isinstance(other, Job):
            return self.next_run < other.next_run
        return False

    def __str__(self):
        dict_status = self.get_status()
        return ",".join(["{}={}".format(k, v) for k, v in dict_status.items()])

    def __repr__(self):
        return "Job: {}, {}, {}".format(
            str(self.job_id), self._job_type, self.status)

    def get_status(self) -> Dict:
        """
        :return: Dictionary of Job current status
        :rtype: Dict
        """
        dict_result = {"id": self.job_id,
                       "job_type": self._job_type,
                       "job_function": self._job_func.__name__,
                       "args": self._job_func.args,
                       "kwargs": self._job_func.keywords,
                       "interval": self._interval,
                       "marked_for_cancel": self._mark_for_abortion.is_set(),
                       "status": self.status.value,
                       "next_run": self.next_run,
                       "last_run_started": self._last_run_started,
                       "last_run_ended": self._last_run_ended,
                       "last_run_duration": self.last_run_duration,
                       "iterations": self._iterations,
                       "is_repeatable": self._is_repeatable,
                       "continue": self.can_continue(),
                       "period": self._period,
                       "last_result": self._last_result}

        return dict_result

    @property
    def last_run_duration(self):
        """
        :return: Last run duration
        """
        try:
            return self._last_run_ended - self._last_run_started
        except TypeError:
            return None

    def cleanup(self):
        """
        Cleanup function that can be overriden
        """
        Logger.info("Cleanup: {} [{}]".format(self._job_type, self.job_id))

    def _do_job(self):
        Logger.info("Running: {} [{}]".format(self._job_type, self.job_id))
        self._last_result = None

    def can_continue(self) -> bool:
        """
        A job can continue to the next iteration:
         - If it's repeatable
         - If it's not TERMINATED or FAILED(?)

        :return: True iff job can continue to the next iteration
        :rtype: bool
        """
        return self._is_repeatable and self.status not in \
               [JobStatusEnum.TERMINATED, JobStatusEnum.FAILED]

    def run_job(self):
        if self._mark_for_abortion.is_set():
            return self.abort()

        self._iterations += 1
        self.status = JobStatusEnum.RUNNING
        self._last_run_started = datetime.now()
        try:
            Logger.info("Running job: {}, #{}".format(self, self._iterations))
            rc = self._job_func()
            self._last_run_ended = datetime.now()
            self.status = JobStatusEnum.SUCCEEDED
            self._schedule_next_run()

        except Exception as e:
            self.status = JobStatusEnum.FAILED
            Logger.warning(str(e))
            Logger.warning(traceback.format_exc())
            rc = str(e)

        return rc

    def abort(self):
        """
        Abort Job
         - Only one thread will do it
         - All other threads will return
         - Simply sets status to TERMINATED
         - Scheduler should understand that it shouldn't reschedule it
        """
        with self._abort_lock:
            if self._mark_for_abortion.is_set():
                # All other threads will avoid cancellation
                Logger.info("Job already cancelled: {}".format(self.job_id))
                return

            # Only one thread will cancel the job
            if self.status in [JobStatusEnum.TERMINATED, JobStatusEnum.FAILED]:
                Logger.warning("Job is already cancelled")
                return

            if self.status in [JobStatusEnum.RUNNING]:
                Logger.warning(
                    "Job is already running - cannot abort it until finished")
                return

            # Only when job can be cancelled, we will raise a flag
            self._mark_for_abortion.set()

            Logger.info("Abort a Job: {}".format(self.job_id))
            self.status = JobStatusEnum.TERMINATED

    @property
    def should_run(self):
        """
        :return: True iff job should run
        :rtype: bool
        """
        return datetime.now() >= self.next_run

    def _schedule_next_run(self):
        """
        Schedule next job's run.
         - Sets next run to be: current timestamp + interval
        """
        self.next_run = datetime.now() + self._period


class BackupJob(Job):
    """
    Example backup job
    """

    def __init__(self, interval: int, time_unit, *args, **kwargs):
        Job.__init__(self, job_type=JobTypeEnum.BACKUP, interval=interval,
                     time_unit=time_unit, *args, **kwargs)


class RegressionJob(Job):
    """
    Example regression job
    """

    def __init__(self, interval: int, time_unit, *args, **kwargs):
        Job.__init__(self, job_type=JobTypeEnum.REGRESSION, interval=interval,
                     time_unit=time_unit, *args, **kwargs)


class BuildJob(Job):
    """
    Example build job
    """

    def __init__(self, interval: int, time_unit, *args, **kwargs):
        Job.__init__(self, job_type=JobTypeEnum.BUILD, interval=interval,
                     time_unit=time_unit, *args, **kwargs)


"""
Job Factory
"""
JOB_OBJECTS = {JobTypeEnum.BUILD: BuildJob,
               JobTypeEnum.REGRESSION: RegressionJob,
               JobTypeEnum.BACKUP: BackupJob}


def _job_factory(job_type: JobTypeEnum):
    assert isinstance(job_type, JobTypeEnum), \
        "job_type must be of type 'JobTypeEnum'"

    try:
        return JOB_OBJECTS[job_type]

    except KeyError:
        jobs_types = list(JobTypeEnum.__members__.values())
        raise AssertionError(
            "Invalid job_type ({}): accepts only one of the following: '{}'"
                .format(job_type,
                        ", ".join(str(e) for e in jobs_types))) from None


# Singleton
JOB_FACTORY = _job_factory
