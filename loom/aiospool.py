import asyncio
import warnings
import sys
import traceback

import tqdm
from tqdm.contrib import DummyTqdmFile


class AIOSpool():

    """A spool is a queue of threads.
    This is a simple way of making sure you aren't running too many threads at one time.
    At intervals, determined by `delay`, the spooler (if on) will start threads from the queue.
    The spooler can start multiple threads at once.

    You can .print to this object, and it will intelligently print the arguments
    based on whether or not it's using a progress bar.
    """

    def __init__(self, quota=8, jobs=None, name="AIOSpool", use_progbar=True):
        """Create a spool

        Args:
            quota (int): Size of quota, i.e. how many threads can run at once.
            cfinish (dict, optional): Description
        """
        jobs = jobs or []

        self.name = name
        self.use_progbar = use_progbar
        self.quota = quota

        self.started_threads = []
        self.queue = []

        self._pbar_max = 0

        self.nop = (lambda: None)
        self.on_finish_callbacks = [
            self.doSpool
        ]

        if isinstance(jobs, int):
            warnings.warn("Jobs should be iterable, not an int! You're using queue syntax!")
            jobs = []

        if iter(jobs) and not isinstance(jobs, str):
            for job in jobs:
                self.enqueue(job)
        else:
            warnings.warn("Jobs should be iterable! You're using the wrong init syntax!")

    async def __aenter__(self):
        return self

    async def __aexit__(self, type_, value, traceback):
        try:
            return await self.finish(resume=False)
        except KeyboardInterrupt:
            print("Spool got KeyboardInterrupt")
            self.background_spool = False
            self.queue = []
            raise

    def __str__(self):
        return f"{type(self)} at {hex(id(self))}: {self.numActiveJobs} active jobs."

    # Interfaces

    def print(self, *args, **kwargs):
        if self.progbar:
            self.progbar.write(
                kwargs.get("sep", " ").join(args)
            )
        else:
            print(*args, **kwargs)

    async def finish(self, resume=False, verbose=False, use_pbar=None):
        """Block and complete all threads in queue.

        Args:
            resume (bool, optional): Resume spooling after finished
            verbose (bool, optional): Report progress towards queue completion.
            use_pbar (bool, optional): Graphically display progress towards queue completions
        """
        if use_pbar is None:
            use_pbar = self.use_progbar

        if verbose:
            print(self)

        # Progress bar management, optional.
        # Wrap the existing callback
        def updateProgressBar():
            # Update progress bar.
            if use_pbar:
                q = (len(self.queue) if self.queue else 0)
                progress = (self._pbar_max - (self.numActiveJobs + q))

                progbar.total = self._pbar_max
                progbar.n = progress
                progbar.set_postfix(queue=q, waiting=f"{self.numActiveJobs:2}]")
                progbar.update(0)

        self.on_finish_callbacks.insert(0, updateProgressBar)

        self._pbar_max = self.numActiveJobs + len(self.queue)

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

                while self.numActiveJobs:
                    await asyncio.gather(*self.started_threads)
                updateProgressBar()

                if not self.numActiveJobs == 0:
                    raise AssertionError("Finished without finishing all threads")
            finally:
                if use_pbar:
                    progbar.close()
                    sys.stdout, sys.stderr = orig_out_err

        if verbose:
            print(self)

        self.on_finish_callbacks.remove(updateProgressBar)

    def on_finish_callback(self):
        for c in self.on_finish_callbacks:
            c()

    def doSpool(self):
        # While there is a queue
        # This gets called from an ending job, so we don't count that one.
        while len(self.queue) > 0 and (self.numActiveJobs - 1) < self.quota:
            try:
                # print(f"Starting job {len(self.queue)=} {self.numActiveJobs=} {self.quota=}")
                future = self.queue.pop()
                self.started_threads.append(asyncio.ensure_future(future))

            except IndexError:
                print(f"IndexError: Popped from empty queue?\nWhile queueing thread {len(self.queue)}-{self.quota}-{self.numRunningThreads}")
                break
            # threads_to_queue = min(len(self.queue), self.quota - self.numRunningThreads)

    def enqueue(self, target):
        """Add a thread to the back of the queue.

        Args:
            target (function): The function to execute
            name (str): Name of thread, for debugging purposes
            args (tuple, optional): Description
            kwargs (dict, optional): Description

            *thargs: Args for threading.Thread
            **thkwargs: Kwargs for threading.Thread
        """
        async def runAndFlag():
            try:
                await target
            except:
                print("Aborting spooled job", file=sys.stderr)
                traceback.print_exc()
            finally:
                self.on_finish_callback()
        self.queue.append(runAndFlag())
        self._pbar_max += 1
        self.on_finish_callback()

    ##################
    # Minor utility
    ##################

    @property
    def numActiveJobs(self):
        """Accurately count number of "our" running threads.

        Returns:
            int: Number of running threads owned by this spool
        """
        return len([
            thread
            for thread in self.started_threads
            if not thread.done()
        ])
