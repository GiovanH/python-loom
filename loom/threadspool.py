#!/bin/python3

import threading
import traceback
import sys

import tqdm
from tqdm.contrib import DummyTqdmFile


class ThreadSpool():

    """A spool is a queue of threads.
    This is a simple way of making sure you aren't running too many threads at one time.
    At intervals, determined by `delay`, the spooler (if on) will start threads from the queue.
    The spooler can start multiple threads at once.

    You can .print to this object, and it will intelligently print the arguments
    based on whether or not it's using a progress bar.
    """

    def __init__(self, quota=8, name="Spool", belay=False, use_progbar=True):
        """Create a spool

        Args:
            quota (int): Size of quota, i.e. how many threads can run at once.
            cfinish (dict, optional): Description
        """
        super().__init__()
        self.quota = quota
        self.name = name
        self.use_progbar = use_progbar

        self.queue = []
        self.started_threads = []

        self.flushing = 0
        self._pbar_max = 0
        self.spoolThread = None
        self.background_spool = False
        self.may_have_room = threading.Event()

        if not belay:
            self.start()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        try:
            self.finish(resume=False)
        except KeyboardInterrupt:
            print("Spool got KeyboardInterrupt")
            self.background_spool = False
            self.queue = []
            raise

    def __str__(self):
        return f"{type(self)} at {hex(id(self))}: {self.numRunningThreads}/{self.quota} threads running with {len(self.queue)} queued."

    # Interfaces

    def print(self, *args, **kwargs):
        if self.progbar:
            self.progbar.write(
                kwargs.get("sep", " ").join(args)
            )
        else:
            print(*args, **kwargs)

    def start(self):
        """Begin spooling threads in the background, if not already doing so.
        """
        self.background_spool = True
        if not (self.spoolThread and self.spoolThread.is_alive()):
            self.spoolThread = threading.Thread(target=self.spoolLoop, name="Spooler")
            self.spoolThread.start()
            self.may_have_room.set()

    def cancel(self):
        """Abort immeditately, potentially without finishing threads.
        """
        self.queue.clear()
        self.finish()

    def finish(self, resume=False, verbose=False, use_pbar=None):
        """Block and complete all threads in queue.

        Args:
            resume (bool, optional): Resume spooling after finished
            verbose (bool, optional): Report progress towards queue completion.
            use_pbar (bool, optional): Graphically display progress towards queue completions
        """
        if use_pbar is None:
            use_pbar = self.use_progbar

        # Stop existing spools
        self.background_spool = False
        self.may_have_room.set()  # If we were paused before

        # if self.spoolThread.isAlive:
        #     raise AssertionError("Background loop did not terminate")

        if verbose:
            print(self)

        # Progress bar management, optional.
        def updateProgressBar():
            # Update progress bar.
            if use_pbar:
                q = (len(self.queue) if self.queue else 0)
                progress = (self._pbar_max - (self.numRunningThreads + q))
                # progress = (self._pbar_max - q)
                if progress < 0:
                    # print(f"{progress=} {self._pbar_max=} {self.numRunningThreads=} {q=}")
                    progress = 0

                progbar.total = self._pbar_max
                progbar.n = progress
                progbar.set_postfix(queue=q, running=f"{self.numRunningThreads:2}/{self.quota}]")
                progbar.update(0)

        self._pbar_max = self.numRunningThreads + (len(self.queue) if self.queue else 0)

        if self._pbar_max > 0:

            try:
                if use_pbar:
                    orig_out_err = sys.stdout, sys.stderr
                    sys.stdout, sys.stderr = map(DummyTqdmFile, orig_out_err)
                    self.progbar = progbar = tqdm.tqdm(
                        file=orig_out_err[0], dynamic_ncols=True,
                        desc=self.name,
                        total=self._pbar_max,
                        unit="job"
                    )

                    updateProgressBar()

                # Create a spoolloop, but block until it deploys all threads.
                while (self.queue and len(self.queue) > 0) or (self.numRunningThreads > 0):
                    self.may_have_room.wait()
                    self.doSpool(verbose=False, callbacks=[updateProgressBar])
                updateProgressBar()

                if not len(self.queue) == 0:
                    raise AssertionError("Finished without deploying all threads")
                if not self.numRunningThreads == 0:
                    raise AssertionError("Finished without finishing all threads")

            finally:
                if use_pbar:
                    progbar.close()
                    sys.stdout, sys.stderr = orig_out_err

        if resume:
            self.queue.clear()  # Create a fresh queue
            self.start()

        if verbose:
            print(self)

    def flush(self):
        """Start and finishes all current threads before starting any new ones.
        """
        self.flushing = 1

    def enqueue(self, target, args=None, kwargs=None, *thargs, **thkwargs):
        """Add a thread to the back of the queue.

        Args:
            target (function): The function to execute
            name (str): Name of thread, for debugging purposes
            args (tuple, optional): Description
            kwargs (dict, optional): Description

            *thargs: Args for threading.Thread
            **thkwargs: Kwargs for threading.Thread
        """
        args = args or ()
        kwargs = kwargs or {}

        def runAndFlag():
            try:
                target(*args, **kwargs)
            except:  # noqa: E722
                print("Aborting spooled thread", file=sys.stderr)
                traceback.print_exc()
            finally:
                self.may_have_room.set()

        self.queue.append(threading.Thread(*thargs, **{'target': runAndFlag, **thkwargs}))
        self._pbar_max += 1
        self.may_have_room.set()

    def setQuota(self, new_quota):
        self.quota = new_quota
        self.may_have_room.set()

    ##################
    # Minor utility
    ##################

    def startThread(self, new_thread):
        self.started_threads.append(new_thread)
        new_thread.start()
        # self.may_have_room.set()

    @property
    def numRunningThreads(self):
        """Accurately count number of "our" running threads.

        Returns:
            int: Number of running threads owned by this spool
        """
        return len([
            thread
            for thread in self.started_threads
            if thread.is_alive()
        ])

    ##################
    # Spooling
    ##################

    def spoolLoop(self, verbose=False):
        """Periodically start additional threads, if we have the resources to do so.
        This function is intended to be run as a thread.
        Runs until the queue is empty or, if self.background_spool is true, runs forever.

        Args:
            verbose (bool, optional): Report progress towards queue completion.
        """

        while self.background_spool:
            self.may_have_room.wait()
            # print("Spoolloop checking to exit", self.background_spool)
            if not self.background_spool:
                break
            #   self.may_have_room.set()
            self.doSpool(verbose=verbose)
        # print("Terminating spoolLoop")

    def doSpool(self, verbose=False, callbacks=None):
        """Spools new threads until the queue empties or the quota fills.

        Args:
            verbose (bool, optional): Verbose output
        """

        callbacks = callbacks or []

        if self.flushing == 1:
            # Finish running threads
            if self.numRunningThreads == 0:
                self.flushing = 0
            else:
                # self.may_have_room.clear()
                return

        # Start threads until we've hit quota, or until we're out of threads.
        while len(self.queue) > 0 and self.quota - self.numRunningThreads > 0:

            if verbose:
                print(self)
            threads_to_queue = min(len(self.queue), self.quota - self.numRunningThreads)
            for i in range(threads_to_queue):
                try:
                    self.startThread(self.queue.pop())
                except IndexError:
                    print(f"Race warning: Popped from empty queue?\nWhile queueing thread {len(self.queue)}-{self.quota}-{self.numRunningThreads}")
                    break

            for callback in callbacks:
                callback()

        self.may_have_room.clear()
