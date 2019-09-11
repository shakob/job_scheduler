#!/usr/bin/python3

import os
import tempfile
from time import sleep
from unittest import TestCase

from scheduler import Logger
from scheduler.thread_pool import ThreadPool


class TestThreadPool(TestCase):
    TEST_FILE = "test.log"
    ABS_FILE_PATH = os.path.join(tempfile.gettempdir(), TEST_FILE)

    def func_to_run(self, contents):
        Logger.info("Running function")
        with open(TestThreadPool.ABS_FILE_PATH, "w+") as file_fd:
            file_fd.write(contents)

    def test_sanity(self):
        sch = ThreadPool(size=1)
        self.assertEqual(len(sch), 1)
        sch.start()
        self.assertEqual(len(sch), 1)
        sch.stop()
        self.assertEqual(len(sch), 0)

    def test_run_pool(self):
        sch = ThreadPool(size=1)
        sch.start()

        contents = '123'

        try:
            sch.run(self.func_to_run, contents=contents)

            sleep(1)
            try:
                with open(TestThreadPool.ABS_FILE_PATH, "r") as file_fd:
                    list_found = file_fd.read()
                    self.assertEqual(list_found, contents)
            finally:
                try:
                    os.remove(TestThreadPool.ABS_FILE_PATH)
                except FileNotFoundError:
                    pass
        finally:
            sch.stop()
