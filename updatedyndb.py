#!/bin/python
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

table = dynamodb.Table('dwxtpcds-9n4f-dwx-managed')

exclusiveStartKey = None

while True:
    print("Start key is " + str(exclusiveStartKey))
    if exclusiveStartKey == None:
        response = table.scan(
            FilterExpression='is_dir=:t AND is_authoritative=:f AND NOT begins_with(child, :cbw)',
            ExpressionAttributeValues={ ":t": True, ":f": False, ":cbw": "_tmp"}
        )
    else:
        response = table.scan(
            FilterExpression='is_dir=:t AND is_authoritative=:f AND NOT begins_with(child, :cbw)',
            ExpressionAttributeValues={ ":t": True, ":f": False, ":cbw": "_tmp"},
            ExclusiveStartKey=exclusiveStartKey
        )
    items=response['Items']
    if 'LastEvaluatedKey' in response:
        exclusiveStartKey=response['LastEvaluatedKey']
    else:
        exclusiveStartKey=None
    print("Found " + str(len(items)) + " items to update")
    print("Last key evaluated in this batch is " + str(exclusiveStartKey))
    for item in items:
        try:
            response = table.update_item(
                Key={"parent": item['parent'], "child": item['child']},
                UpdateExpression='SET #is_aut = :is_aut',
                ExpressionAttributeNames={
                    "#is_aut": 'is_authoritative'
                },
                ExpressionAttributeValues={
                    ":is_aut": True
                }
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print("Item ignored ")
                continue
            else:
                raise
    if exclusiveStartKey == None:
        break
