import pytest


PROCESSOR = 'datapackage_pipelines_od4tj.processors.fix_numbers'


class TestFixNumbers(object):
    def test_factor_multiplies_number_by_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'number',
                        'factor': '1m',
                    }
                ]
            }
        }
        row = {
            'value': 1,
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {}, {}
        )

        assert result['value'] == row['value'] * 1000000

    def test_converts_integer_numbers_to_int(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'value': 1,
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {}, {}
        )

        assert result['value'] == 1
        assert not isinstance(result['value'], float)

    def test_factor_ignores_fields_without_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'value': 1,
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {}, {}
        )

        assert result['value'] == row['value']

    def test_factor_raises_when_schema_has_invalid_factor(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'number',
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

    def test_convert_parenthesis_to_negative(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'value': '(100)'
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {}, {}
        )

        assert result['value'] == -100

    def test_convert_parenthesis_to_negative_decimal(self, processor):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'value': '(100,420.31)'
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {}, {}
        )

        assert result['value'] == -100420.31

    @pytest.mark.parametrize('null_value', [
        '-',
        '',
        'string',
    ])
    def test_removes_null_values(self, processor, null_value):
        # We also remove any string, otherwise the "set_types" processor will fail
        # (Our data is very messy and contain strings mixed with numbers in the same column).
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'value': null_value
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {}, {}
        )

        assert result['value'] is None

    @pytest.mark.parametrize('value', [
        None,
        True,
    ])
    def test_ignores_non_numeric_values(self, processor, value):
        spec = {
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'number',
                    }
                ]
            }
        }
        row = {
            'value': value
        }

        result = processor.process_row(
            row.copy(), 0, spec, 0, {}, {}
        )

        assert result['value'] == value
