from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

def process_resource(rows, missing_countries):
    raw_field = parameters['raw_field']
    clean_field = parameters['clean_field']

    missing_values = []

    for row in rows:
        if row[clean_field]:
            yield row
        else:
            raw_value = row[raw_field]
            if raw_value:
                missing_values.append({
                    'missing_value': raw_value,
                    'year': row['year'],
                    'entity': row['entity']
                })


def process_resources(resources):
    first = next(resources)
    missing_countries = []
    yield process_resource(first, missing_countries)
    yield missing_countries


def modify_datapackage(dp, *_):
    dp['resources'].append({
        'name': 'missing_countries',
        'path': 'data/missing_countries.csv',
        'schema': {
            'fields': [
                {'name': 'year',   'type': 'integer'},
                {'name': 'entity', 'type': 'string'},
                {'name': 'missing_value', 'type': 'string'},
            ]
        }
    })
    return dp

if __name__ == '__main__':
    dp = modify_datapackage(dp)
    spew(dp, process_resources(res_iter))
