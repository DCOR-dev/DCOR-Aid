import logging
import os

from ._version import version


def setup_logging(module="dcoraid", level=None):
    """Setup global logging

    The logging `level` can be any logging level (e.g. `logging.INFO`).

    If the logging level is not set, then `INFO` is the default. If the
    environment variable `CSKERNEL_DEBUG` is set or if CSKernel is
    in-between releases ("post" in the version string), then `DEBUG`
    is used.
    """
    is_dev = version.count("post")
    must_debug = os.environ.get("DCORAID_DEBUG")
    if level is None:
        level = logging.DEBUG if (is_dev or must_debug) else logging.INFO

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(processName)s/%(threadName)s "
            "in %(name)s: %(message)s",
        datefmt="%H:%M:%S"
    )

    logger = logging.getLogger(module)
    handler_stream = logging.StreamHandler()
    handler_stream.setFormatter(formatter)
    logger.addHandler(handler_stream)
    logger.setLevel(level)
