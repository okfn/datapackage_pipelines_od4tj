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

            pipeline = [
                {
                    'run': 'add_metadata',
                    'parameters': {
                        'name': '{}_{}'.format(entity_slug, item['year']),
                        'title': 'CRD/IV data for {entity} in the year {year}'.format(**item)
                    },
                },
                {
                    'run': 'add_resource',
                    'parameters': {
                        'name': 'country-codes',
                        'url': 'https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv'
                    },
                },
                {
                    'run': 'stream_remote_resources',
                },
                {
                    'run': 'od4tj.prepare_country_fingerprints',
                    'parameters': {
                        'resource-name': 'country-codes',
                        'source-fields': ['name', 'official_name_en', 'official_name_fr'],
                        'name-field': 'name',
                        'fingerprint-field': 'fingerprint'
                    }
                }
            ]
            for input in item['inputs']:
                if input['kind'] == 'pdf':
                    for dimension in input['parameters']['dimensions']:
                        parameters = {}
                        parameters['dimensions'] = dimension
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
                    'run': 'od4tj.fingerprint_countries',
                    'parameters': {
                        'resource-name': 'crdiv_data',
                        'name-field': 'country',
                        'fingerprint-field': 'country-name-fingerprint'
                    }
                },
                {
                    'run': 'join',
                    'parameters': {
                        'source': {
                            'name': 'country-codes',
                            'key': ['fingerprint'],
                            'delete': True
                        },
                        'target': {
                            'name': 'crdiv_data',
                            'key': ['country-name-fingerprint'],
                        },
                        'fields': {
                            'country_name': {
                                'name': 'name'
                            }
                        },
                        'full': False,
                    }
                },
                {
                    'run': 'od4tj.add_constants',
                    'parameters': {
                        'year': item['year'],
                        'entity': item['entity']
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
            yield pipeline_id, {
                'pipeline': pipeline
            }
