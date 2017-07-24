# pylama:ignore=E501
import os
import shutil
import tempfile

import itertools
import tabula
import requests

from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()
header_names = [h['name'] for h in parameters['headers']]


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
    if parameters['skip_empty_cells']:
        data = [[x for x in row if x] for row in data]
        data = [row for row in data if row]
    data = [dict(zip(header_names, row)) for row in data]
    list(extractor)
    return data


def modify_datapackage(dp):
    fields = parameters['headers']
    for f in fields:
        f['type'] = f.get('type', 'string')
    resource = {
        'name': 'tabula-' + parameters['dimensions']['selection_id'].lower(),
        'path': 'data/{}.csv'.format(parameters['dimensions']['selection_id']),
        'schema': {
            'fields': fields
        }
    }
    dp['resources'].append(resource)
    return dp


spew(modify_datapackage(dp),
     itertools.chain(res_iter, [tabula_extract(fetch_pdf_file())]))
