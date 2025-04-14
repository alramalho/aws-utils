import argparse
import json
import time
from pprint import pprint

import boto3


def scan_table_page(table_name, aws_endpoint=None, limit=100, exclusive_start_key=None):
    """Scan a single page of items from a DynamoDB table."""
    if aws_endpoint:
        client = boto3.client('dynamodb', endpoint_url=aws_endpoint)
    else:
        client = boto3.client('dynamodb')

    scan_params = {'TableName': table_name, 'Limit': limit}
    
    if exclusive_start_key:
        scan_params['ExclusiveStartKey'] = exclusive_start_key
    
    return client.scan(**scan_params)


def write_batch_items(table_name, items, aws_endpoint=None):
    """Write a batch of items to a DynamoDB table."""
    if aws_endpoint:
        client = boto3.client('dynamodb', endpoint_url=aws_endpoint)
    else:
        client = boto3.client('dynamodb')
    
    request_items = {
        table_name: [{'PutRequest': {'Item': item}} for item in items]
    }
    
    return client.batch_write_item(RequestItems=request_items)


def migrate_table(source_table, dest_table, aws_endpoint=None, batch_size=25, max_items=None):
    """
    Migrate data from source table to destination table with pagination.
    
    Args:
        source_table (str): Source DynamoDB table name
        dest_table (str): Destination DynamoDB table name
        aws_endpoint (str, optional): AWS endpoint URL. Defaults to None.
        batch_size (int, optional): Batch size for writes (max 25). Defaults to 25.
        max_items (int, optional): Maximum number of items to migrate. Defaults to None (all items).
        
    Returns:
        int: Number of items processed
    """
    items_processed = 0
    batches_processed = 0
    last_key = None
    
    print(f"Starting migration from {source_table} to {dest_table}")
    
    # Keep scanning and writing until all items are processed
    while True:
        # Scan a page of items from the source table
        response = scan_table_page(
            source_table, 
            aws_endpoint, 
            limit=min(batch_size, 100), 
            exclusive_start_key=last_key
        )
        
        items = response.get('Items', [])
        
        if not items:
            print("No more items to process")
            break
        
        # Process the items in batches of 25 (DynamoDB batch write limit)
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            write_batch_items(dest_table, batch, aws_endpoint)
            items_processed += len(batch)
            batches_processed += 1
            
            print(f"Processed {items_processed} items ({batches_processed} batches)")
            
            # Respect the maximum items limit if specified
            if max_items and items_processed >= max_items:
                print(f"Reached maximum items limit ({max_items})")
                return items_processed
            
            # Small pause to avoid hitting rate limits
            time.sleep(0.1)
        
        # Get the last evaluated key for pagination
        last_key = response.get('LastEvaluatedKey')
        
        if not last_key:
            print("Migration completed - processed all items")
            break
    
    print(f"Migration completed - processed {items_processed} items in {batches_processed} batches")
    return items_processed


def main():
    parser = argparse.ArgumentParser(description='Migrate data from one DynamoDB table to another.')
    parser.add_argument('-s', '--source_table', type=str, help='Source table name', required=True)
    parser.add_argument('-d', '--dest_table', type=str, help='Destination table name', required=True)
    parser.add_argument('-e', '--aws_endpoint', type=str, help='AWS endpoint URL (optional, for local development)')
    parser.add_argument('-b', '--batch_size', type=int, default=25, help='Batch size for writes (max 25)')
    parser.add_argument('-m', '--max_items', type=int, help='Maximum number of items to migrate')
    args = parser.parse_args()
    
    # Validate batch size
    if args.batch_size > 25:
        print("Warning: Maximum batch size is 25, using 25 instead of", args.batch_size)
        args.batch_size = 25
    
    # Run the migration
    total_items = migrate_table(
        args.source_table,
        args.dest_table,
        args.aws_endpoint,
        args.batch_size,
        args.max_items
    )
    
    print(f"Successfully migrated {total_items} items from {args.source_table} to {args.dest_table}")

if __name__ == '__main__':
    main() 