from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def process_resource(rows):

    expected_totals = dict(
        (k, v * parameters['factor'])
        for k, v in parameters['totals'].items()
    )
    total_fields = expected_totals.keys()
    totals = dict(
        (f, 0)
        for f in total_fields
    )
    ERROR_MARGIN = 1 * parameters['factor']

    for row in rows:
        for f in total_fields:
            if row[f]:
                totals[f] += row[f]
        yield row

    for f in total_fields:
        assert abs(totals[f] - expected_totals[f]) <= ERROR_MARGIN, \
                "%r: Expected total of %s, got %s instead" % \
                (f, expected_totals[f], totals[f])


def process_resources(resources):
    first = next(resources)
    yield process_resource(first)
    yield from resources


if __name__ == '__main__':
    spew(dp, process_resources(res_iter))
