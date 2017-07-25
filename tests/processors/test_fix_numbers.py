import pytest


PROCESSOR = 'datapackage_pipelines_od4tj.processors.fix_numbers'


class TestFixNumbers(object):
    def test_factor_multiplies_number_by_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'turnover',
                        'type': 'string',
                    }
                ]
            }
        }
        row = {
            'turnover': 1,
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {'factor': 1000000 }, {}
        )

        assert result['turnover'] == row['turnover'] * 1000000

    def test_converts_integer_numbers_to_int(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'turnover',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'turnover': "1",
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {'factor': 1}, {}
        )

        assert result['turnover'] == 1
        assert not isinstance(result['turnover'], float)

    def test_factor_ignores_fields_without_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'turnover',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'turnover': 1,
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {'factor': 1}, {}
        )

        assert result['turnover'] == row['turnover']

    def test_factor_raises_when_schema_has_invalid_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'turnover',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'turnover': 1,
        }

        with pytest.raises(KeyError):
            processor.process_row(row, 0, spec, 0, { 'factor': 'INVALID_FACTOR'}, {})

    def test_convert_parenthesis_to_negative(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'turnover',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'turnover': '(100)'
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {'factor': 1}, {}
        )

        assert result['turnover'] == -100

    def test_convert_parenthesis_to_negative_decimal(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'turnover',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'turnover': '(100,420.31)'
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {'factor': 1}, {}
        )

        assert result['turnover'] == -100420.31

    @pytest.mark.parametrize('null_value', [
        '-',
        '',
    ])
    def test_removes_null_values(self, processor, null_value):
        # We also remove any string, otherwise the "set_types" processor will fail
        # (Our data is very messy and contain strings mixed with numbers in the same column).
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'turnover',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'turnover': null_value
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {'factor': 1}, {}
        )

        assert result['turnover'] is None

    @pytest.mark.parametrize('value', [
        None,
        True,
    ])
    def test_ignores_non_numeric_values(self, processor, value):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'turnover',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'turnover': value
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {'factor': 1}, {}
        )

        assert result['turnover'] == value
