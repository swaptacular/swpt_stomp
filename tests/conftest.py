import pytest


@pytest.fixture
def datadir(request):
    import os.path

    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)
    return {
        'AA': os.path.join(test_dir, '../test_data/AA'),
        'CA': os.path.join(test_dir, '../test_data/CA'),
        'DA': os.path.join(test_dir, '../test_data/DA'),
    }
