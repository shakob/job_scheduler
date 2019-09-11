#!/usr/bin/python3

from queue import Queue, Empty
from threading import Thread, Event

from scheduler import Logger

__all__ = ["ThreadPool"]


class ThreadPool:
    """
    Thread pool class
     - Contains number of Worker threads
     - Maintains a jobs queue
     - Workers takes jobs from queue and run them
    """

    def __init__(self, size: int):
        """
        :param size: Max number of threads
        :type size: int
        """
        self._jobs_queue = Queue(-1)
        self._list_workers = []
        for index in range(size):
            worker = ThreadPool.Worker(index=index + 1,
                                       jobs_queue=self._jobs_queue)
            self._list_workers.append(worker)

    def start(self):
        """
        Start Thread pool
        """
        for worker in self._list_workers:
            worker.start()

    def __len__(self):
        """
        :return: Number of worker threads
        """
        return len(self._list_workers)

    def __del__(self):
        """
        Stops worker threads.
        Clears Pool
        """
        return self.stop()

    def __str__(self):
        return "ThreadPool: Size={}".format(self.__len__())

    def run(self, func, *args, **kwargs):
        """
        Add function to jobs queue

        :param func: Function to run
        :param args: args
        :param kwargs: kwargs
        """
        self._jobs_queue.put((func, args, kwargs), block=True)

    def wait_for_completion(self):
        """
        Waits for all workers to finish
        """
        self._jobs_queue.join()

    def stop(self):
        """
        Stop Pool
         - Wait for all worker threads to finish
         - Clears thread pool
        :return:
        """
        for worker in self._list_workers:
            # 1 to avoid stuck - Workers are zombie threads
            try:
                worker.join(timeout=1)
            except RuntimeError:
                # Worker wasn't started - ignore
                pass

        Logger.info("Thread Pool stopped")
        self._list_workers.clear()

    class Worker(Thread):
        """
        Actual worker in pool
        - Pool inner class
        """

        # Default timeout to sleep when no job was found
        # Wakes up and checks if it can continue running
        _TIMEOUT = 2

        def __init__(self, index, jobs_queue: Queue):
            """
            :param index: Worker index - used for naming
            :param jobs_queue: Queue of jobs - shared collection
            """
            Thread.__init__(self, daemon=True, name="Worker_{}".format(index))
            self._task_queue = jobs_queue

            self._keep_running = Event()
            self._keep_running.set()

        def join(self, timeout=None):
            """
            Stops Worker

            :param timeout: Timeout to wait. Default None = until finished
            :return: Thread.join RC
            """
            self._keep_running.clear()
            return super().join(timeout=timeout)

        def run(self):
            """
            Worker function
             - Runs until _keep_running event is down
             - Takes a job from the job queue: Blocks for _TIMEOUT seconds
             - Runs the function
             - Prints
            """
            Logger.info("Worker {} .. Start".format(self.name))
            while self._keep_running.is_set():
                try:
                    func, args, kwargs = self._task_queue.get(
                        block=True, timeout=ThreadPool.Worker._TIMEOUT)
                    try:
                        func(*args, **kwargs)
                    except Exception as e:
                        Logger.warning(e)
                    finally:
                        self._task_queue.task_done()

                except Empty:
                    # DO nothing when found no job to do
                    # The wait is part of get job from queue
                    pass

            Logger.info("Worker {} .. Done".format(self.name))
