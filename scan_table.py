import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pprint import pprint

import boto3


def scan_table(table_name, aws_endpoint=None, max_items=10000, last_days=None):
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

    if last_days is not None:
        cutoff_datetime = datetime.now() - timedelta(days=last_days)
        cutoff_str = cutoff_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
        fetched_items = [item for item in fetched_items if item.get('created_at', {}).get('S', '') > cutoff_str]
    
    return {
        "Items": fetched_items
    }

def cluster_and_count(items, cluster_field):
    """Cluster items by a specified field and count the occurrences."""
    cluster_counts = defaultdict(int)
    
    for item in items:
        # Assuming all values are strings, you may need to handle this differently depending on your DynamoDB setup
        key = item.get(cluster_field, {}).get('S', 'None')
        cluster_counts[key] += 1
    
    return cluster_counts

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scan DynamoDB table entries.')
    parser.add_argument('-t', '--table_name', type=str, help='Name of the table to scan.', required=True)
    parser.add_argument('-e', '--aws_endpoint', type=str, help='AWS endpoint URL (optional).')
    parser.add_argument('-m', '--max_items', type=int, help='Maximum number of items to fetch. If not specified, fetches all items.')
    parser.add_argument('-c', '--cluster_by', type=str, help='Cluster by this field and count items by cluster.')
    parser.add_argument('-d', '--last_days', type=int, help='Filter items from the last X days. Assumes a "created_at" field in ISO 8601 format.')
    args = parser.parse_args()
    
    result = scan_table(args.table_name, args.aws_endpoint, args.max_items, args.last_days)
    print("Number of items fetched: {}".format(len(result['Items'])))
    
    if len(result['Items']) > 0 and not args.cluster_by:
        print("First item:")
        pprint(json.dumps(result['Items'][0], indent=4))
    
    if args.cluster_by:
        clusters = cluster_and_count(result['Items'], args.cluster_by)
        print("\nCounts by cluster ({}):".format(args.cluster_by))
        for key, count in clusters.items():
            print("{}: {}".format(key, count))





