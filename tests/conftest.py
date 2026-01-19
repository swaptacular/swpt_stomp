import logging
import os
import asyncio
import pytest


@pytest.fixture
def datadir(request):
    import os.path

    filename = request.module.__file__
    test_dir = os.path.join(filename, "../../test_data")
    return {
        "AA": os.path.join(test_dir, "AA"),
        "CA": os.path.join(test_dir, "CA"),
        "DA": os.path.join(test_dir, "DA"),
    }


@pytest.fixture
def rmq_url(request):
    return os.environ.get(
        "PROTOCOL_BROKER_URL", "amqp://guest:guest@localhost:5672"
    )


@pytest.fixture
def loop(request):
    # NOTE: Without this, we can randomly get harmless "--- Logging
    # error ---" messages from Python's logging code during the tests,
    # because Pytest may happen to close stdout/stderr before the log
    # messages emitted from the background threads reach the logging
    # system. In this case, Python's logging system will try to write
    # to a closed stream. Setting `logging.raiseExceptions` to False
    # prevents the logging system's error messages from cluttering
    # Pytest's output.
    logging.raiseExceptions = False

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop
