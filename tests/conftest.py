import importlib
import pytest
import unittest.mock as mock


@pytest.fixture(scope='module')
def processor(request):
    '''Load the processor named on the module's PROCESSOR attribute.

    We can't load the processor directly because they call methods when
    imported. This fixture mocks those methods and returns the loaded
    processor. To use it, you need to define a `PROCESSOR` constant in the test
    module, outside of any class. This constant has the import path for the
    processor you'd like to import.

    After that, you can use this fixture as you'd use any other.
    '''
    module_name = request.module.PROCESSOR
    with mock.patch('datapackage_pipelines.wrapper.process'):
        yield importlib.import_module(module_name)
