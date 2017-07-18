import json
import os

from datapackage_pipelines.utilities.lib_test_helpers import ProcessorFixtureTestsBase, rejsonize

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')

class ProcessorsFixturesTest(ProcessorFixtureTestsBase):

    def _get_procesor_env(self):
        return {}

    def _get_processor_file(self, processor):
        processor = processor.replace('.', '/')
        return os.path.join(ROOT_PATH,
                            'datapackage_pipelines_od4tj',
                            'processors',
                            processor.strip() + '.py')

    @staticmethod
    def _get_first_line(data):
        """
        Return the first line of `data` as a python object.
        """
        if len(data) > 0:
            data = data[0]
            data = data.split('\n')
            actual = data[0]
            rj_actual = rejsonize(actual)
            return json.loads(rj_actual)


for filename, func in ProcessorsFixturesTest(
        os.path.join(os.path.dirname(__file__), 'fixtures')
).get_tests():
    globals()['test_processors_%s' % filename] = func

