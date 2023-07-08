import pytest


@pytest.fixture
def datadir(request):
    import os.path

    filename = request.module.__file__
    test_dir = os.path.join(filename, '../../test_data')
    return {
        'AA': os.path.join(test_dir, 'AA'),
        'CA': os.path.join(test_dir, 'CA'),
        'DA': os.path.join(test_dir, 'DA'),
    }


@pytest.fixture
def rmq_url(request):
    return 'amqp://guest:guest@localhost:5672'
