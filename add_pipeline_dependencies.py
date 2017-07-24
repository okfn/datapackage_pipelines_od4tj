import os
import yaml
from slugify import slugify

dependencies = []
FN = 'od4tj.source-spec.yaml'
for dirpath, dirnames, filenames in os.walk('.'):
    if FN in filenames:
        spec = yaml.load(open(os.path.join(dirpath, FN)))
        for file in spec:
            pipeline_id = os.path.join(dirpath, slugify(file['entity'], to_lower=True, separator='_'), str(file['year']))
            dependencies.append({'pipeline': pipeline_id})

pipelines = yaml.load(open('datapackage_pipelines_od4tj/pipelines/pipeline-spec.yaml'))
for i in pipelines.values():
    i['dependencies'] = dependencies

yaml.dump(pipelines, open('datapackage_pipelines_od4tj/pipelines/pipeline-spec.yaml', 'wt'))