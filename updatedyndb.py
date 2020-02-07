#!/bin/python
import boto3
from botocore.exceptions import ClientError

DEBUG=True

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

table = dynamodb.Table('dwxtpcds30-wwgq-dwx-managed')

exclusiveStartKey = None

while True:
    print("Start key is " + str(exclusiveStartKey))
    if exclusiveStartKey == None:
        response = table.scan(
            FilterExpression='is_dir=:t AND is_authoritative=:f AND NOT begins_with(child, :cbw) AND NOT begins_with(child, :cbw2)',
            ExpressionAttributeValues={ ":t": True, ":f": False, ":cbw": "_tmp", ":cbw2": "tmp"}
        )
    else:
        response = table.scan(
            FilterExpression='is_dir=:t AND is_authoritative=:f AND NOT begins_with(child, :cbw) AND NOT begins_with(child, :cbw2)',
            ExpressionAttributeValues={ ":t": True, ":f": False, ":cbw": "_tmp", ":cbw2": "tmp"},
            ExclusiveStartKey=exclusiveStartKey
        )
    items=response['Items']
    if 'LastEvaluatedKey' in response:
        exclusiveStartKey=response['LastEvaluatedKey']
    else:
        exclusiveStartKey=None
    print("Last key evaluated in this batch is " + str(exclusiveStartKey))
    itemsUpdated = 0
    for item in items:
        if item['parent'].find('/tmp') > -1 or item['parent'].find('/_tmp') > -1 and item['parent'].find('/das/') > -1:
            continue
        if DEBUG:
            print item['parent'], item['child']
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
            itemsUpdated = itemsUpdated + 1
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print("Item ignored ")
                continue
            else:
                raise
        
    print("Updated " + str(itemsUpdated) + " items")
    if exclusiveStartKey == None:
        break
