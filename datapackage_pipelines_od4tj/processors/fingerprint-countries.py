from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import process

import fingerprints


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    resource_matcher = ResourceMatcher(parameters['resource-name'])
    if resource_matcher.match(spec['name']):
        row[parameters['fingerprint-field']] = fingerprints.generate(row[parameters['name-field']])

    return row


def modify_datapackage(dp, parameters, stats):

    resource_matcher = ResourceMatcher(parameters['resource-name'])

    for res in dp['resources']:
        if resource_matcher.match(res['name']):
            res['schema']['fields'].extend([
                {'name': parameters['fingerprint-field'],
                 'type': 'string'},
            ])
            return dp


process(process_row=process_row,
        modify_datapackage=modify_datapackage)