import logging
import os
import sys
import atexit
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

APP_LOG_LEVEL = os.environ.get('APP_LOG_LEVEL', 'warning')
APP_LOG_FORMAT = os.environ.get('APP_LOG_FORMAT', 'text')
APP_ASSOCIATED_LOGGERS = os.environ.get('APP_ASSOCIATED_LOGGERS', '').split()
APP_ASSOCIATED_LOGGERS.append('__main__')


def _excepthook(exc_type, exc_value, traceback):
    logging.error("Uncaught exception occured",
                  exc_info=(exc_type, exc_value, traceback))


def _remove_handlers(logger) -> None:
    for h in logger.handlers:
        logger.removeHandler(h)


def _create_console_hander(format: str) -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    fmt = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'

    if format == 'text':
        handler.setFormatter(logging.Formatter(fmt))
    elif format == 'json':
        from pythonjsonlogger import jsonlogger
        handler.setFormatter(jsonlogger.JsonFormatter(fmt))
    else:
        raise RuntimeError(f'invalid log format: {format}')

    # Use `handler.addFilter(...)` here, to add log filters.
    return handler


def _configure_root_logger(format: str) -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    _remove_handlers(root_logger)
    log_queue: Queue[logging.LogRecord] = Queue()
    queue_handler = QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)
    console_handler = _create_console_hander(format)
    listener = QueueListener(log_queue, console_handler)
    atexit.register(listener.stop)
    listener.start()
    return root_logger


def configure_logging(
        level: str = APP_LOG_LEVEL,
        format: str = APP_LOG_FORMAT,  # Must be "text" or "json".
        associated_loggers: list[str] = APP_ASSOCIATED_LOGGERS,
) -> None:
    """Configure async logging to stdout.

    After calling this function, the logging will be configured so that all
    log records will be added to a queue, and a separate thread will be
    started to continuously read this queue and write to the standard
    output.
    """
    root_logger = _configure_root_logger(format)

    # Set the log level for this app's logger.
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level.upper())
    app_logger_level = app_logger.getEffectiveLevel()

    # Make sure that all loggers that are associated to this app have their
    # log levels set to the specified level as well.
    for qualname in associated_loggers:
        logging.getLogger(qualname).setLevel(app_logger_level)

    # Make sure that the root logger's log level (that is: the log level for
    # all third party libraires) is not lower than the specified level.
    if app_logger_level > root_logger.getEffectiveLevel():
        root_logger.setLevel(app_logger_level)

    # Make sure uncaught exceptions are logged as errors.
    sys.excepthook = _excepthook
