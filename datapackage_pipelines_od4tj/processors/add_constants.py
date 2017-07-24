from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    row['year'] = parameters['year']
    row['entity'] = parameters['entity']
    row['subsidiary'] = parameters.get('subsidiary')
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {'name': 'year',   'type': 'integer'},
        {'name': 'entity', 'type': 'string'},
        {'name': 'subsidiary', 'type': 'string'},
    ])
    return dp


process(process_row=process_row,
        modify_datapackage=modify_datapackage)
