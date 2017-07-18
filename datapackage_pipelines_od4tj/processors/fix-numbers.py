from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    for f in spec['schema']['fields']:
        if 'factor' in f:
            factor = {
                '1m': 1000000
            }[f['factor']]
        v = row[f['name']]
        if v:
            row[f['name']] = v * factor
    return row


process(process_row=process_row)