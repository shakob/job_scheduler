#!/usr/bin/python3

import time
from enum import Enum
from unittest import TestCase

from scheduler import JobTypeEnum, JobStatusEnum
from scheduler.scheduler import TimeUnitEnum
from scheduler.job import JOB_FACTORY


class JobTests(TestCase):
    class EnumForTesting(Enum):
        A = "A"

    def test_input(self):
        for input_arg in [JobTests.EnumForTesting.A, "123", 123, None,
                          [1, 2, 3], (1, 2, 3), {1: 2, "3": "4"}]:
            with self.assertRaises(AssertionError):
                JOB_FACTORY(input_arg)(interval=12,
                                       time_unit=TimeUnitEnum.SECONDS)

            with self.assertRaises(AssertionError):
                JOB_FACTORY(JobTypeEnum.REGRESSION)(
                    interval=input_arg, time_unit=TimeUnitEnum.HOURS)

            with self.assertRaises(AssertionError):
                JOB_FACTORY(JobTypeEnum.REGRESSION)(interval=3,
                                                    time_unit=input_arg)

    def test_should_run(self):
        job1 = JOB_FACTORY(JobTypeEnum.REGRESSION)\
            (interval=3, time_unit=TimeUnitEnum.SECONDS)
        self.assertFalse(job1.should_run)
        time.sleep(3)
        self.assertTrue(job1.should_run)

    def _test_id(self):
        job1 = JOB_FACTORY(JobTypeEnum.REGRESSION)(
            interval=3, time_unit=TimeUnitEnum.SECONDS)
        self.assertEqual(job1.job_id, 1)
        job1 = JOB_FACTORY(JobTypeEnum.REGRESSION)(
            interval=3, time_unit=TimeUnitEnum.SECONDS)
        self.assertEqual(job1.job_id, 2)

    def test_job_status(self):
        job1 = JOB_FACTORY(JobTypeEnum.REGRESSION)(
            interval=6, time_unit=TimeUnitEnum.SECONDS)
        self.assertEqual(job1.status, JobStatusEnum.PENDING)
        job1.abort()
        self.assertEqual(job1.status, JobStatusEnum.TERMINATED)

    def test_can_continue(self):
        job1 = JOB_FACTORY(JobTypeEnum.REGRESSION)(
            interval=0, time_unit=TimeUnitEnum.SECONDS)
        self.assertEqual(job1.can_continue(), False)

        job2 = JOB_FACTORY(JobTypeEnum.REGRESSION)(
            interval=6, time_unit=TimeUnitEnum.SECONDS)
        self.assertEqual(job2.can_continue(), True)
        job2.abort()
        self.assertEqual(job2.can_continue(), False)
