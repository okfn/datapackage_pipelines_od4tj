from boto import connect_s3
from boto.s3.connection import OrdinaryCallingFormat
from datapackage_pipelines.wrapper import ingest, spew

params, dp, res_iter = ingest()
dp.setdefault('resources', [])

conn = connect_s3(
    is_secure=True,               # require ssl
    calling_format=OrdinaryCallingFormat(),
    host='s3-eu-west-1.amazonaws.com'
)
bucket = conn.get_bucket(params['bucket'])

response = bucket.list(prefix=params['path'])
for resource in response:
    resource_path = resource.name
    if resource_path.endswith(params['suffix']):
        dp['resources'].append({
            'name': resource_path.replace('/', '-'),
            'url': 'http://{0}.s3.amazonaws.com/{1}'
                       .format(params['bucket'], resource_path),
        })

spew(dp, res_iter)
