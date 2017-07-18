import pytest


PROCESSOR = 'datapackage_pipelines_od4tj.processors.fix_numbers'


class TestFixNumbers(object):
    def test_factor_multiplies_number_by_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'factor': '1m',
                    }
                ]
            }
        }
        row = {
            'value': 1,
        }

        result = processor.process_row(
            row, 0, spec, 0, {}, {}
        )

        assert result['value'] == row['value'] * 1000000

    def test_factor_ignores_fields_without_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                    }
                ]
            }
        }
        row = {
            'value': 1,
        }

        result = processor.process_row(
            row, 0, spec, 0, {}, {}
        )

        assert result['value'] == row['value']

    def test_factor_raises_when_schema_has_invalid_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'factor': 'INVALID_FACTOR'
                    }
                ]
            }
        }
        row = {
            'value': 1,
        }

        with pytest.raises(KeyError):
            processor.process_row(row, 0, spec, 0, {}, {})
