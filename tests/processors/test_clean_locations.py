import pytest

PROCESSOR = 'datapackage_pipelines_od4tj.processors.clean_locations'


class TestCleanLocations(object):
    def test_modify_datapackage_add_country_name_field(self, processor):
        parameters = {
            'resource_name': 'foobar',
            'clean_field_code': 'clean_field_code',
            'clean_field_name': 'clean_field_name',
        }
        datapackage = {
            'resources': [{
                'name': parameters['resource_name'],
                'schema': {
                    'fields': []
                }
            }]
        }
        stats = {}

        result_datapackage = processor.modify_datapackage(
            datapackage, parameters, stats
        )

        fields = result_datapackage['resources'][0]['schema']['fields']
        assert fields == [
            {
                'name': parameters['clean_field_code'],
                'type': 'string',
            },
            {
                'name': parameters['clean_field_name'],
                'type': 'string',
            }
        ]

    @pytest.mark.parametrize('country,clean_country_code,clean_country_name', [
        ('United States', 'USA', 'United States'),
        ('United States of America', 'USA', 'United States'),
        ('United Kingdom', 'GBR', 'United Kingdom'),
        ('Unknown country', None, None),
    ])
    def test_process_row_normalize_location_name(self, processor, country, clean_country_code, clean_country_name):
        parameters = {
            'resource_name': 'foobar',
            'clean_field_code': 'clean_field_code',
            'clean_field_name': 'clean_field_name',
            'raw_field': 'country',
        }
        row = {
            parameters['raw_field']: country,
        }
        spec = {
            'name': parameters['resource_name'],
        }

        result_row = processor.process_row(
            row, 0, spec, 0, parameters, {}
        )

        assert result_row == {
            parameters['raw_field']: row[parameters['raw_field']],
            parameters['clean_field_code']: clean_country_code,
            parameters['clean_field_name']: clean_country_name,
        }
