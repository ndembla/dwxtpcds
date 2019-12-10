import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

table = dynamodb.Table('dwxtpcds-9n4f-dwx-managed')

response = table.scan(
ProjectionExpression='#k,#s',
ExpressionAttributeNames={
'#k' : 'parent', #partition key
'#s' : 'child' #sort key
}
)

items = response['Items']

for item in items:
try:
 response = table.update_item(
 Key=item,
 UpdateExpression='SET #a = :ath',
 ConditionExpression='#d = :t and NOT begins_with(#s, :tmp) and #a = :f',
 ExpressionAttributeNames={
 "#d" : 'is_dir',
 "#a" : 'is_authoritative',
 "#s" : 'child'
 },
 ExpressionAttributeValues={
 ":ath" : True,
 ":t" : True,
 ":f" : False,
 ":tmp": '_tmp'
 }
 )
 print("Updated item")
except ClientError as e:
    if e.response['Error']['Code'] == "ConditionalCheckFailedException":
print ("Item ignored ")
continue
    else:
raise
