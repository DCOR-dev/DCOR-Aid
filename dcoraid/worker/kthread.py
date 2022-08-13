"""https://github.com/munshigroup/kthread"""
import atexit
import ctypes
import time
import threading


class KThreadExit(BaseException):
    pass


def _async_raise(tid, exctype):
    """Raises the exception, causing the thread to exit"""
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(tid), ctypes.py_object(exctype))
    if res == 0:
        pass  # ignore
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class KThread(threading.Thread):
    """Killable thread. See terminate() for details."""

    def __init__(self, *args, **kwargs):
        super(KThread, self).__init__(*args, **kwargs)
        atexit.register(self.terminate)

    def _get_my_tid(self):
        """Determines the instance's thread ID"""
        if not self.is_alive():
            return None  # Thread is not active

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("Could not determine the thread's ID")

    def raise_exc(self, exctype):
        """raises the given exception type in the context of this thread"""
        thread_id = self._get_my_tid()
        if thread_id:
            _async_raise(thread_id, exctype)

    def terminate(self):
        """raises SystemExit in the context of the given thread, which should
        cause the thread to exit silently (unless caught)"""
        # WARNING: using terminate() can introduce instability in your
        # programs. It is worth noting that terminate() will NOT work if the
        # thread in question is blocked by a syscall (accept(), recv(), etc.).
        atexit.unregister(self.terminate)
        while self.is_alive():
            self.raise_exc(KThreadExit)
            time.sleep(.1)
