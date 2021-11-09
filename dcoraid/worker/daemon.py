import logging
import traceback

import time

from ..common import ConnectionTimeoutErrors

from .kthread import KThread


class Daemon(KThread):
    def __init__(self, queue, job_trigger_state, job_function_name):
        """Daemon base class for running uploads/downloads in the background"""
        self.queue = queue
        self.state = "running"
        self.job_trigger_state = job_trigger_state
        self.job_function_name = job_function_name
        super(Daemon, self).__init__()
        self.daemon = True  # We don't have to worry about ending this thread
        self.start()

    def join(self, *args, **kwargs):
        """Join thread by breaking the while loop"""
        self.state = "exiting"
        super(Daemon, self).join(*args, **kwargs)
        assert self.state == "exited"

    def run(self):
        while True:
            if self.state == "exiting":
                self.state = "exited"
                break
            elif self.state != "running":
                # Don't do anything
                time.sleep(.1)
                continue
            else:
                # Get the first job that is in the trigger state
                for job in self.queue:
                    if job.state == self.job_trigger_state:
                        break
                else:
                    # Nothing to do, sleep a little to avoid 100% CPU
                    time.sleep(.1)
                    continue
                # Perform daemon task
                task = getattr(job, self.job_function_name)
                logger = logging.getLogger(__name__)
                try:
                    task()
                except ConnectionTimeoutErrors:
                    # Set the job to the error state for 10s (so the user
                    # sees it in the UI) and then go back to the initial
                    # job trigger state.
                    job.set_state("error")
                    job.traceback = traceback.format_exc(limit=1) \
                        + "\nDCOR-Aid will retry in 10s!"
                    logger.error(
                        f"(dataset {job.dataset_id}) {traceback.format_exc()}")
                    time.sleep(10)
                    job.set_state(self.job_trigger_state)
                except SystemExit:
                    job.set_state("abort")
                    logger.error(f"(dataset {job.dataset_id}) Aborted!")
                except BaseException:
                    # Set job to error state and let the user figure
                    # out what to do next.
                    job.set_state("error")
                    job.traceback = traceback.format_exc()
                    logger.error(
                        f"(dataset {job.dataset_id}) {traceback.format_exc()}")
