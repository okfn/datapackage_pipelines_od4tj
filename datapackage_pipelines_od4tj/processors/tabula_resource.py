# pylama:ignore=E501
import os
import re
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


def columns_for_headers(resource):
    num_columns = len(resource[0])
    columns_for_headers = {}
    for header in parameters['headers']:
        scores = [0] * num_columns
        logging.info('LOOKING %r for title %r', header['mapping'], header['title'])
        for row in resource:
            for col in range(num_columns):
                row_value = re.sub(r'-\r|\n', '', row[col])
                row_value = re.sub(r'\s', ' ', row_value)
                scores[col] += 1 if fuzz.partial_ratio(row[col], header['title']) > SCORE_THRESHOLD else 0
        maxcol, maxscore = max(enumerate(scores), key=lambda x: x[1])
        logging.info('HEADER %r got column %d', header['mapping'], maxcol)
        assert maxcol not in columns_for_headers, \
            "header %r and %r mapped to same column %d" % (header, columns_for_headers[maxcol], maxcol)
        assert maxscore > 0, "Failed to find column for header %r" % header
        columns_for_headers[maxcol] = header['mapping']
    return columns_for_headers


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
    data = []
    dimensions = parameters['dimensions']
    for selection in dimensions:
        tabula_params['pages'] = selection['page']
        # Warning: `spreadsheet=False` doesn't have the same effect as `nospreadsheet=True`
        if selection.get('extraction_method') == 'spreadsheet' or selection.get('extraction_method') == 'lattice':
            tabula_params['spreadsheet'] = True
        else:
            tabula_params['nospreadsheet'] = True
        tabula_params['area'] = [
            selection['y1'],
            selection['x1'],
            selection['y2'],
            selection['x2'],
        ]
        r = tabula.read_pdf(parameters['url'], **tabula_params)
        selection_data = r[0]['data']
        selection_data = [[cell['text'] for cell in row] for row in selection_data]
        if parameters['transpose']:
            selection_data = list(map(list, zip(*selection_data)))
        data.extend(selection_data)
    return data


def add_headers(resource, extracted_headers):
    new_resource = []
    for row in resource:
        new_row = {}
        for index, value in enumerate(row):
            if index in extracted_headers:
                new_row[extracted_headers[index]] = value
        new_row['url'] = parameters['url']
        new_resource.append(new_row)
    return new_resource


def process_resource():
    resource_data = tabula_extract(fetch_pdf_file())
    resource_headers = columns_for_headers(resource_data)
    return add_headers(resource_data, resource_headers)


def modify_datapackage(dp):
    fields = parameters['headers']
    filename = os.path.splitext(os.path.basename(parameters['url']))[0]
    resource = {
        'name': 'tabula-{}'.format(re.sub(r'\s', '', filename.lower())),
        'path': 'data/{}.csv'.format(filename),
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
    ])
    dp['resources'].append(resource)
    return dp


spew(modify_datapackage(dp), itertools.chain(res_iter, [process_resource()]))
