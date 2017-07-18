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
    dimensions = parameters['dimensions']
    pages = dimensions['page']
    area = [
        dimensions['y1'],
        dimensions['x1'],
        dimensions['y2'],
        dimensions['x2'],
    ]
    r = tabula.read_pdf(filename, pages=pages, area=area, output_format='json', guess=False)
    data = r[0]['data']
    data = [dict(zip(header_names, [cell['text'] for cell in row])) for row in data]
    list(extractor)
    return data


def modify_datapackage(dp):
    fields = parameters['headers']
    for f in fields:
        if 'type' not in f:
            # if 'factor' in f:
            #     f['type'] = 'number'
            #     if 'groupChar' not in f:
            #         f['groupChar'] = ','
            # else:
                f['type'] = 'string'
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
