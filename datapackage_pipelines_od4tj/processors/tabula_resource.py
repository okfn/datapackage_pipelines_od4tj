# pylama:ignore=E501
import os
import shutil
import tempfile

import itertools

import logging
import tabula
import requests

from fuzzywuzzy import fuzz

from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()
headers = parameters['headers']

HEADER_SEARCH_ROWS = 10
SCORE_THRESHOLD = 80

def fetch_pdf_file():
    url = parameters['url']
    delete_after_extract = False
    if url.startswith('http'):
        pdf_file = tempfile.NamedTemporaryFile(delete=False)
        stream = requests.get(url, stream=True).raw
        shutil.copyfileobj(stream, pdf_file)
        pdf_file.close()
        pdf_file = pdf_file.name
        delete_after_extract = True
    else:
        pdf_file = url

    yield pdf_file

    if delete_after_extract:
        os.unlink(pdf_file)


def tabula_extract(extractor):
    filename = next(extractor)
    tabula_params = {
        'guess': False,
        'output_format': 'json',
    }
    dimensions = parameters['dimensions']
    tabula_params['pages'] = dimensions['page']
    # Warning: `spreadsheet=False` doesn't have the same effect as `nospreadsheet=True`
    if dimensions.get('extraction_method') == 'lattice':
        tabula_params['spreadsheet'] = True
    else:
        tabula_params['nospreadsheet'] = True
    tabula_params['area'] = [
        dimensions['y1'],
        dimensions['x1'],
        dimensions['y2'],
        dimensions['x2'],
    ]
    r = tabula.read_pdf(filename, **tabula_params)
    data = r[0]['data']
    data = [[cell['text'] for cell in row] for row in data]
    if parameters['transpose']:
        data = list(map(list, zip(*data)))

    num_columns = len(data[0])
    column_for_header = {}
    for header in headers:
        scores = [0] * num_columns
        logging.info('LOOKING %r for title %r', header['mapping'], header['title'])
        for row in data[:HEADER_SEARCH_ROWS]:
            for col in range(num_columns):
                scores[col] += 1 if fuzz.partial_ratio(row[col], header['title']) > SCORE_THRESHOLD else 0
        maxcol, maxscore = max(enumerate(scores), key=lambda x: x[1])
        logging.info('HEADER %r got column %d', header['mapping'], maxcol)
        assert maxcol not in column_for_header, \
            "header %r and %r mapped to same column %d" % (header, column_for_header[maxcol], maxcol)
        assert maxscore > 0, "Failed to find column for header %r" % header
        column_for_header[maxcol] = header['mapping']

    data = [dict((column_for_header[i], x)
                 for i, x in enumerate(row)
                 if i in column_for_header)
            for row in data]
    for row in data:
        row['url'] = parameters['url']
        row['page'] = dimensions['page']
    list(extractor)
    return data


def modify_datapackage(dp):
    fields = parameters['headers']
    resource = {
        'name': 'tabula-' + parameters['dimensions']['selection_id'].lower(),
        'path': 'data/{}.csv'.format(parameters['dimensions']['selection_id']),
        'schema': {
            'fields': [
                {
                    'name': f['mapping'],
                    'type': 'string'
                }
                for f in fields
            ]
        }
    }
    resource['schema']['fields'].extend([
        {'name': 'url', 'type': 'string'},
        {'name': 'page', 'type': 'integer'},
    ])
    dp['resources'].append(resource)
    return dp


spew(modify_datapackage(dp),
     itertools.chain(res_iter, [tabula_extract(fetch_pdf_file())]))
