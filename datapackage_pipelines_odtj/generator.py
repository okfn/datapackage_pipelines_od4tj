import os
import json
import pkgutil

from datapackage_pipelines.generators import (
    GeneratorBase,
    steps,
    slugify,
    SCHEDULE_DAILY
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
        # TODO Add code here
        for item in source:
            entity_slug = slugify(item['entity'], to_lower=True, separator='_')
            pipeline_id = '{}/{}'.format(entity_slug, item['year'])

            pipeline = [{
                'run': 'add_metadata',
                'parameters': {
                    'name': '{}_{}'.format(entity_slug, item['year']),
                    'title': 'CRD/IV data for {entity} in the year {year}'.format(**item)
                }
            }]
            for input in item['inputs']:
                if input['kind'] == 'pdf':
                    for dimension in input['parameters']['dimensions']:
                        parameters = {}
                        parameters['dimensions'] = dimension
                        parameters['url'] = input['url']
                        parameters['headers'] = item['model']['headers']
                        pipeline.append({
                            'run': 'odtj.tabula-resource',
                            'parameters': parameters
                        })
            # pipeline.append({
            #     'run': 'dump.to_path',
            #     'parameters': {
            #         'out-path': '.'
            #     }
            # })
            yield pipeline_id, {
                'pipeline': pipeline
            }
