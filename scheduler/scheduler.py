#!/usr/bin/python3
import time
from functools import partial, update_wrapper
from threading import Lock, Thread, Event
from typing import List

from scheduler import TimeUnitEnum, Logger, JobTypeEnum, ThreadSafeCounter
from scheduler.job import Job, JOB_FACTORY
from scheduler.thread_pool import ThreadPool

__all__ = ["Scheduler", "TestingScheduler"]


class Scheduler(Thread):
    """
    Scheduler class:
     - Thread: Runs forever until joined
     -
    """

    def __init__(self, max_jobs: int):
        Thread.__init__(self, name="Scheduler", daemon=False)
        self._list_jobs: List[Job] = []
        self._jobs_lock = Lock()

        self._keep_running = Event()
        self._keep_running.set()

        self._thread_pool = ThreadPool(size=max_jobs)
        self.time_unit = TimeUnitEnum.HOURS

    def __len__(self):
        """
        :return: Size of thread pool
        :rtype: int
        """
        return len(self._thread_pool)

    def get_status(self):
        dict_result = {"time_unit": self.time_unit,
                       "is_running": self._keep_running.is_set(),
                       "jobs": [job.get_status() for job in self._list_jobs]}

        return dict_result

    def _pre_run(self):
        pass

    def _post_run(self):
        pass

    def run(self):
        """
        Runs forever until joined
        :return:
        """
        self._thread_pool.start()

        Logger.info("Scheduler Start")
        while self._keep_running.is_set():
            self._pre_run()
            self._run_pending()
            time.sleep(1)
            self._post_run()

        Logger.info("Scheduler Stopped")

    def join(self, timeout=None):
        self._keep_running.clear()
        self._thread_pool.stop()

        return super().join(timeout=timeout)

    def _run_pending(self):
        """
        Run pending jobs that should run
         - Should run:: current time > Job's scheduled time
         - Put each job in job queue of thread pool
         - Checks that each Job can continue to the next run
        """
        with self._jobs_lock:
            list_jobs_to_iterate = self._list_jobs

        Logger.info(str(list_jobs_to_iterate))

        list_jobs_to_run = []
        list_remaining_jobs = []
        for job in list_jobs_to_iterate:
            if job.should_run:
                list_jobs_to_run.append(job)
            else:
                list_remaining_jobs.append(job)

        with self._jobs_lock:
            self._list_jobs = list_remaining_jobs

        list_jobs_to_run = [job for job in list_jobs_to_iterate if
                            job.should_run]

        list_jobs_to_run = sorted(list_jobs_to_run)

        list_threads = [Thread(target=self._thread_pool.run,
                               kwargs={"func": job.run_job},
                               daemon=True)
                        for job in list_jobs_to_run]

        for job_thread in list_threads:
            job_thread.start()

        for job_thread in list_threads:
            job_thread.join()

        for job in list_remaining_jobs:
            if not job.can_continue():
                job.cleanup()

        for job in list_jobs_to_run:
            if job.can_continue():
                with self._jobs_lock:
                    self._list_jobs.append(job)
            else:
                job.cleanup()

    def add_job(self, interval: int, job_type: JobTypeEnum, *args, **kwargs):
        job = JOB_FACTORY(job_type)
        j = job(interval=interval, time_unit=self.time_unit, *args, **kwargs)

        with self._jobs_lock:
            self._list_jobs.append(j)

        Logger.info("Add a new Job: {}, interval: {} {}"
                    .format(j.job_id, interval, self.time_unit.value))

        return j

    @staticmethod
    def _get_partial(func, *args, **kwargs):
        job_func = partial(func, *args, **kwargs)
        try:
            update_wrapper(job_func, func)
        except AttributeError:
            # job_funcs already wrapped by partial won't have
            # __name__, __module__ or __doc__ and the update_wrapper()
            # call will fail.
            pass

        return job_func


class TestingScheduler(Scheduler):
    def __init__(self, max_jobs: int):
        Scheduler.__init__(self, max_jobs=max_jobs)
        self.time_unit = TimeUnitEnum.SECONDS

        self._cycles = ThreadSafeCounter()

    def _pre_run(self):
        self._cycles.next()

    def _post_run(self):
        pass

    @property
    def cycles(self):
        """
        :return: Number of cycles scheduler has made
        :rtype: int
        """
        return self._cycles.value
