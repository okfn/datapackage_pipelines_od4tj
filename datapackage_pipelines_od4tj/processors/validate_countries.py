from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

def process_resource(rows, missing_countries):
    for row in rows:
        if row['country_name']:
            yield row
        else:
            country = row['country']
            if country:
                missing_countries.append({
                    'missing-country': country,
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
        'name': 'missing-counries',
        'path': 'data/missing-countries.csv',
        'schema': {
            'fields': [
                {'name': 'year',   'type': 'integer'},
                {'name': 'entity', 'type': 'string'},
                {'name': 'missing-country', 'type': 'string'},
            ]
        }
    })
    return dp

if __name__ == '__main__':
    dp = modify_datapackage(dp)
    spew(dp, process_resources(res_iter))
