import atexit
import logging
import threading
import traceback

import time

from ..common import ConnectionTimeoutErrors

from .kthread import KThread, KThreadExit


class Daemon(KThread):
    def __init__(self,
                 jobs,
                 job_trigger_state: str,
                 job_function_name: str,
                 job_function_kwargs: dict = None,
                 ):
        """Daemon base class for running uploads/downloads in the background"""
        self.jobs = jobs
        self.job_trigger_state = job_trigger_state
        self.job_function_name = job_function_name
        self.job_function_kwargs = job_function_kwargs
        super(Daemon, self).__init__()
        self.daemon = True  # We don't have to worry about ending this thread

        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self.shutdown_flag = threading.Event()

        atexit.register(self.shutdown_flag.set)

        self.start()

    def run(self):
        try:
            while not self.shutdown_flag.is_set():
                # Get the first job that is in the trigger state
                for job in self.jobs:
                    if job.state == self.job_trigger_state:
                        break
                else:
                    # Nothing to do, sleep a little to avoid 100% CPU
                    time.sleep(.05)
                    continue
                # Perform daemon task
                task = getattr(job, self.job_function_name)
                logger = logging.getLogger(__name__)
                try:
                    if self.job_function_kwargs is None:
                        task()
                    else:
                        task(**self.job_function_kwargs)
                except ConnectionTimeoutErrors:
                    # Set the job to the error state for 10s (so the user
                    # sees it in the UI) and then go back to the initial
                    # job trigger state.
                    job.set_state("error")
                    job.traceback = traceback.format_exc(limit=1) \
                        + "\nDCOR-Aid will retry in 10s!"
                    logger.error(
                        f"(dataset {job.id}) {traceback.format_exc()}")
                    time.sleep(10)
                    job.set_state(self.job_trigger_state)
                except KThreadExit:
                    job.set_state("abort")
                    logger.error(f"{job.__class__.__name__} {job.id} Aborted!")
                except SystemExit:
                    # nothing to do
                    self.terminate()
                except BaseException:
                    if not self.shutdown_flag.is_set():
                        # Only log if the thread is supposed to be running.
                        # Set job to error state and let the user figure
                        # out what to do next.
                        job.set_state("error")
                        job.traceback = traceback.format_exc()
                        logger.error(
                            f"(dataset {job.id}) {traceback.format_exc()}")
        except KThreadExit:
            # killed by KThread
            pass
        except SystemExit:
            self.terminate()
