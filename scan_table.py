import argparse
import json
from pprint import pprint

import boto3


def scan_table(table_name, aws_endpoint=None, max_items=None):
    if aws_endpoint:
        dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint)
        client = boto3.client('dynamodb', endpoint_url=aws_endpoint)
    else:
        dynamodb = boto3.resource('dynamodb')
        client = boto3.client('dynamodb')

    fetched_items = []
    last_evaluated_key = None

    # While loop to handle pagination
    while True:
        scan_params = {'TableName': table_name}
        
        # If we had a LastEvaluatedKey from the previous scan, use it to continue scanning
        if last_evaluated_key:
            scan_params['ExclusiveStartKey'] = last_evaluated_key
        
        # If max_items is specified, then adjust the Limit parameter of the scan
        if max_items:
            remaining_items = max_items - len(fetched_items)
            scan_params['Limit'] = remaining_items

        response = client.scan(**scan_params)
        
        fetched_items.extend(response.get('Items', []))
        
        last_evaluated_key = response.get('LastEvaluatedKey', None)

        # Break out of the loop if there are no more items to scan or if we've reached the max_items
        if not last_evaluated_key or (max_items and len(fetched_items) >= max_items):
            break
    
    return {
        "Items": fetched_items
    }

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scan DynamoDB table entries.')
    parser.add_argument('-t', '--table_name', type=str, help='Name of the table to scan.')
    parser.add_argument('-e', '--aws_endpoint', type=str, help='AWS endpoint URL (optional).')
    parser.add_argument('-m', '--max_items', type=int, help='Maximum number of items to fetch. If not specified, fetches all items.')
    args = parser.parse_args()
    result = scan_table(args.table_name, args.aws_endpoint, args.max_items)
    print("Number of items fetched: {}".format(len(result['Items'])))
    if len(result['Items']) > 0:
        print("First item:")
        pprint(json.dumps(result['Items'][0], indent=4))
