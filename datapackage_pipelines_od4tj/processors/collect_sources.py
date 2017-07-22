import boto3
import logging
from datapackage_pipelines.wrapper import ingest, spew

params, dp, res_iter = ingest()
dp.setdefault('resources', [])

s3client = boto3.client('s3')
truncated = True
continuation_token = None
aws_params = {
    'Bucket': params['bucket'],
    'Prefix': params['path'],
}

while truncated is True:
    if continuation_token is not None:
        aws_params['ContinuationToken'] = continuation_token
    response = s3client.list_objects_v2(**aws_params)
    truncated = response['IsTruncated']
    continuation_token = response.get('NextContinuationToken')
    for resource in response['Contents']:
        resource_path = resource['Key']
        if resource_path.endswith('.csv'):
            dp['resources'].append({
                'name': resource_path.replace('/', '-'),
                'url': 'https://{0}.s3.amazonaws.com/{1}'.format(params['bucket'], resource_path),
            })

spew(dp, res_iter)
