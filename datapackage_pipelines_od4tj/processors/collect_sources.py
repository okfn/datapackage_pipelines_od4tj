import os

from datapackage_pipelines.wrapper import spew, ingest


params, dp, res_iter = ingest()

base = params['base_path']

count = 0
for dirpath, dirnames, filenames in os.walk(base):
    for filename in filenames:
        if filename.endswith('.csv'):
            dp['resources'].append({
                'name': 'res{}'.format(len(dp['resources'])),
                'url': os.path.join(dirpath, filename)
            })

spew(dp, res_iter)