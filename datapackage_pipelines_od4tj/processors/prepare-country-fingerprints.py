import fingerprints
import logging

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, dp, res_iter = ingest()

resource_name = parameters['resource-name']
resource_matcher = ResourceMatcher(resource_name)
source_fields = parameters['source-fields']
name_field = parameters['name-field']
fingerprint_field = parameters['fingerprint-field']


def process_resource(res):
    all_fingerprints = set()
    for row in res:
        name = None
        for src_field in source_fields:
            src_value = row[src_field]
            if src_value:
                if name is None:
                    name = src_value
                fingerprint = fingerprints.generate(src_value)
                if fingerprint in all_fingerprints:
                    continue
                all_fingerprints.add(fingerprint)
                yield {
                    name_field: name,
                    fingerprint_field: fingerprint
                }


def process_resources(resources):
    for res in resources:
        if resource_matcher.match(res.spec['name']):
            yield process_resource(res)
        else:
            yield res


def process_datapackage(dp):
    for res in dp['resources']:
        if resource_matcher.match(res['name']):
            res['schema']['fields'] = [
                {'name': fingerprint_field, 'type': 'string'},
                {'name': name_field, 'type': 'string'},
            ]
    return dp

spew(process_datapackage(dp),
     process_resources(res_iter))
