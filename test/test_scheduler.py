#!/usr/bin/python3

import time
from unittest import TestCase

from scheduler import TimeUnitEnum, JobTypeEnum
from scheduler.scheduler import TestingScheduler, Scheduler


class SchedulerTests(TestCase):
    MAX_JOBS = 5

    def test_sanity(self):
        test_scheduler = TestingScheduler(max_jobs=SchedulerTests.MAX_JOBS)
        self.assertEqual(test_scheduler.time_unit, TimeUnitEnum.SECONDS)
        self.assertEqual(len(test_scheduler), SchedulerTests.MAX_JOBS)

        scheduler = Scheduler(max_jobs=SchedulerTests.MAX_JOBS)
        self.assertEqual(scheduler.time_unit, TimeUnitEnum.HOURS)
        self.assertEqual(len(scheduler), SchedulerTests.MAX_JOBS)

        self.assertFalse(scheduler.is_alive())
        self.assertFalse(scheduler.daemon)

    def test_single_start_stop(self):
        test_scheduler = TestingScheduler(max_jobs=1)
        test_scheduler.start()
        test_scheduler.join()
        self.assertEqual(len(test_scheduler), 0)
        self.assertFalse(test_scheduler.is_alive())

    def test_single_single_job(self):
        scheduler = TestingScheduler(max_jobs=1)
        self.assertEqual(scheduler.cycles, 0)
        scheduler.start()

        job = scheduler.add_job(interval=1, job_type=JobTypeEnum.REGRESSION)
        time.sleep(3)
        self.assertEqual(scheduler.cycles, 3)
        job.abort()

        scheduler.join()

    def _test_temp(self):
        test_scheduler = TestingScheduler(max_jobs=SchedulerTests.MAX_JOBS)
        test_scheduler.start()

        self.assertEqual(test_scheduler.time_unit, TimeUnitEnum.SECONDS)

        job1 = test_scheduler.add_job(job_type=JobTypeEnum.BACKUP, interval=1)
        job2 = test_scheduler.add_job(job_type=JobTypeEnum.BUILD, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.REGRESSION, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.BACKUP, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.BUILD, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.REGRESSION, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.BACKUP, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.BUILD, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.REGRESSION, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.BACKUP, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.BUILD, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.REGRESSION, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.BACKUP, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.BUILD, interval=1)
        test_scheduler.add_job(job_type=JobTypeEnum.REGRESSION, interval=1)
        # job1.cancel_job()

        itr = 0
        is_aborted = False
        is_added = False

        while itr < 20:
            test_scheduler._run_pending()
            time.sleep(1)
            itr += 1
            if not is_aborted and itr > 5:
                job1.abort()
                is_aborted = True

            if not is_added and itr > 8:
                test_scheduler.add_job(job_type=JobTypeEnum.REGRESSION,
                                       interval=3)
                is_added = True
                job2.abort()
                test_scheduler.add_job(job_type=JobTypeEnum.BUILD, interval=6)
