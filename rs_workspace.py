import json
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from pprint import pprint

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

print('rs_workspace.py')
# import pdb; pdb.set_trace()

# objs = s3.list_objects(Bucket='udacity-dend', Prefix='log_data')
# for obj in objs['Contents']:
#     if not obj['Key'].endswith('.json'): continue
#     pprint('object: Key=%s' % obj['Key'])

# objs1 = s3.list_objects(Bucket='udacity-dend', Prefix='log_data')
# objs2 = list(map(lambda obj: obj['Key'], objs1['Contents']))
# objs3 = list(filter(lambda str: str.endswith('.json'), objs2))
# pprint(objs3)

# obj = s3.get_object(Bucket='udacity-dend', Key=objs3[0])
# pprint(obj)
# # str1 = json.loads(str(obj['Body'].read(), 'utf-8'))
# str1 = obj['Body'].read().decode('utf-8').strip()
# df = pd.read_json(str1, lines=True)
# print()
# pprint(df)

# objs1 = s3.list_objects(Bucket='udacity-dend', Prefix='song_data', MaxKeys=500)
paginator = s3.get_paginator('list_objects')
results = paginator.paginate(Bucket='udacity-dend', Prefix='song_data')
total_records = 0
for page in results:
    # pprint(objs1)
    print("len(page['Contents'])=" + str(len(page['Contents'])))
    total_records += len(page['Contents'])
print('total_records=' + str(total_records))
# objs2 = list(map(lambda obj: obj['Key'], objs1['Contents']))
# objs3 = list(filter(lambda str: str.endswith('.json'), objs2))
# pprint(objs3)

print('done')