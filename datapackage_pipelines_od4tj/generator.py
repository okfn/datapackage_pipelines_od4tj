# pylama:ignore=E501
import os
import json

from datapackage_pipelines.generators import (
    GeneratorBase,
    slugify,
)

import logging
log = logging.getLogger(__name__)


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/spec_schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        for item in source:
            entity_slug = slugify(item['entity'], to_lower=True, separator='_')
            pipeline_id = '{}/{}'.format(entity_slug, item['year'])
            dataset_name = '{}_{}'.format(entity_slug, item['year'])
            if 'subsidiary' in item:
                pipeline_id += '/{}'.format(item['subsidiary'])
                dataset_name += '_{}'.format(item['subsidiary'])

            pipeline = [
                {
                    'run': 'add_metadata',
                    'parameters': {
                        'name': dataset_name,
                        'title': 'CRD/IV data for {entity} in the year {year}'.format(**item)
                    },
                },
            ]
            for input in item['inputs']:
                if input['kind'] == 'pdf':
                    for dimension in input['parameters']['dimensions']:
                        parameters = {}
                        parameters['dimensions'] = dimension
                        parameters['transpose'] = input.get('transpose', False)
                        parameters['skip_empty_cells'] = input.get('skip_empty_cells', False)
                        parameters['url'] = input['url']
                        parameters['headers'] = item['model']['headers']
                        pipeline.append({
                            'run': 'od4tj.tabula_resource',
                            'parameters': parameters
                        })
            pipeline.append({
                'run': 'concatenate',
                'parameters': {
                    'sources': 'tabula-.+',
                    'target': {
                        'name': 'crdiv_data'
                    },
                    'fields': dict(
                        (h['name'], [])
                        for h in item['model']['headers']
                    )
                }
            })
            pipeline.extend([
                {
                    'run': 'od4tj.clean_locations',
                    'parameters': {
                        'resource_name': 'crdiv_data',
                        'raw_field': 'country',
                        'clean_field_code': 'country_code',
                        'clean_field_name': 'country_name',
                    }
                },
                {
                    'run': 'od4tj.add_constants',
                    'parameters': {
                        'year': item['year'],
                        'entity': item['entity'],
                        'subsidiary': item.get('subsidiary')
                    }
                },
                {
                    'run': 'od4tj.validate_countries',
                    'parameters': {
                        'resource_name': 'crdiv_data',
                        'raw_field': 'country',
                        'clean_field': 'country_code',
                    }
                },
                {
                    'run': 'od4tj.fix_numbers',
                },
                {
                    'run': 'set_types',
                },
            ])
            pipeline.append({
                'run': 'aws.dump.to_s3',
                'parameters': {
                    'bucket': 'od4tj-filestore.okfn.org',
                    'path': 'crd_iv_datapackages/{}_{}'.format(entity_slug, item['year'])
                }
            })
            pipeline.append({
                'run': 'dump.to_path',
                'parameters': {
                    'out-path': '/tmp/',
                }
            })
            yield pipeline_id, {
                'pipeline': pipeline
            }
