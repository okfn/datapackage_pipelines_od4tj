import pycountry
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import process

from datapackage_pipelines.generators import slugify


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    resource_matcher = ResourceMatcher(parameters['resource_name'])
    if resource_matcher.match(spec['name']):
        clean_field_code = parameters['clean_field_code']
        clean_field_name = parameters['clean_field_name']
        raw_field = parameters['raw_field']
        clean_value_code = None
        clean_value_name = None
        try:
            country = pycountry.countries.lookup(row[raw_field])
            clean_value_code = country.alpha_3
            clean_value_name = country.name
        except LookupError:
            # Ignore values we don't know how to clean
            pass
        row[clean_field_code] = clean_value_code
        row[clean_field_name] = clean_value_name

    return row


def modify_datapackage(dp, parameters, stats):

    resource_matcher = ResourceMatcher(parameters['resource_name'])

    for res in dp['resources']:
        if resource_matcher.match(res['name']):
            res['schema']['fields'].extend([
                {
                    'name': parameters['clean_field_code'],
                    'type': 'string',
                },
                {
                    'name': parameters['clean_field_name'],
                    'type': 'string',
                },
            ])
            return dp


process(process_row=process_row,
        modify_datapackage=modify_datapackage)
