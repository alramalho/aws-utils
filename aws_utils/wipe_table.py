import boto3
import argparse
import time
import sys
import json

def delete_table_entries(table_name, aws_endpoint=None, verbose=False, primary_key=None):
    """
    Delete all entries from a DynamoDB table with verification of items deleted.
    
    Args:
        table_name (str): Name of the DynamoDB table
        aws_endpoint (str, optional): AWS endpoint URL. Defaults to None.
        verbose (bool, optional): Enable verbose output. Defaults to False.
        primary_key (str, optional): Primary key name. If not provided, will be auto-detected.
        
    Returns:
        tuple: (success, initial_count, deleted_count, remaining_count)
    """
    # Set up DynamoDB resources
    if aws_endpoint:
        dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint)
        client = boto3.client('dynamodb', endpoint_url=aws_endpoint)
    else:
        dynamodb = boto3.resource('dynamodb')
        client = boto3.client('dynamodb')
    
    table = dynamodb.Table(table_name)
    
    # Get the primary key name from the table description if not provided
    key_name = primary_key
    if key_name is None:
        try:
            response = client.describe_table(TableName=table_name)
            key_name = response['Table']['KeySchema'][0]['AttributeName']
            if verbose:
                print(f"Auto-detected primary key: {key_name}")
        except Exception as e:
            print(f"Error accessing table {table_name}: {str(e)}")
            return (False, 0, 0, 0)
    
    # Verify primary key is in the schema
    try:
        response = client.describe_table(TableName=table_name)
        schema_key = response['Table']['KeySchema'][0]['AttributeName']
        if schema_key != key_name:
            print(f"Error: Primary key '{key_name}' does not match schema key '{schema_key}'")
            return (False, 0, 0, 0)
    except Exception as e:
        print(f"Error verifying primary key schema: {str(e)}")
    
    # Count items before deletion
    try:
        count_response = client.scan(
            TableName=table_name,
            Select='COUNT'
        )
        initial_count = count_response['Count']
        if verbose:
            print(f"Initial item count: {initial_count}")
    except Exception as e:
        print(f"Error counting items in table {table_name}: {str(e)}")
        return (False, 0, 0, 0)
    
    # Skip if table is already empty
    if initial_count == 0:
        if verbose:
            print(f"Table {table_name} is already empty. No items to delete.")
        return (True, 0, 0, 0)
    
    # Get a sample item to verify the primary key structure
    try:
        sample_response = client.scan(TableName=table_name, Limit=1)
        if sample_response['Items']:
            sample_item = sample_response['Items'][0]
            if key_name in sample_item:
                sample_key = sample_item[key_name]
                if verbose:
                    print(f"Sample primary key: {key_name} = {sample_key}")
            else:
                print(f"Error: Sample item does not contain primary key '{key_name}'")
                print(f"Sample item: {json.dumps(sample_item, default=str)}")
                return (False, initial_count, 0, initial_count)
    except Exception as e:
        print(f"Error getting sample item: {str(e)}")
    
    # Delete all items
    deleted_count = 0
    try:
        # Scan and delete in batches
        last_evaluated_key = None
        
        while True:
            # Scan parameters
            scan_params = {'TableName': table_name}
            if last_evaluated_key:
                scan_params['ExclusiveStartKey'] = last_evaluated_key
            
            scan_response = client.scan(**scan_params)
            items = scan_response.get('Items', [])
            
            if not items:
                break
                
            # Delete items in batches
            with table.batch_writer() as batch:
                for item in items:
                    if key_name not in item:
                        if verbose:
                            print(f"Warning: Item missing primary key '{key_name}', skipping")
                            print(f"Item: {json.dumps(item, default=str)}")
                        continue
                    
                    try:
                        batch.delete_item(Key={key_name: item[key_name][list(item[key_name].keys())[0]]})
                        deleted_count += 1
                        
                        if verbose and deleted_count % 100 == 0:
                            print(f"Deleted {deleted_count} items so far...")
                    except Exception as e:
                        print(f"Error deleting item: {str(e)}")
                        print(f"Item key: {key_name} = {item[key_name]}")
                        if verbose:
                            print(f"Full item: {json.dumps(item, default=str)}")
            
            # Check for more items
            last_evaluated_key = scan_response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
                
            # Small pause to avoid throttling
            time.sleep(0.1)
            
    except Exception as e:
        print(f"Error deleting items from table {table_name}: {str(e)}")
        # Try to get the current count to see how many were deleted
        try:
            count_response = client.scan(
                TableName=table_name,
                Select='COUNT'
            )
            current_count = count_response['Count']
            deleted_count = initial_count - current_count
        except:
            pass
            
        return (False, initial_count, deleted_count, initial_count - deleted_count)
    
    # Verify all items were deleted by checking the count again
    try:
        time.sleep(1)  # Give DynamoDB a moment to process
        count_response = client.scan(
            TableName=table_name,
            Select='COUNT'
        )
        remaining_count = count_response['Count']
        
        if verbose:
            print(f"Remaining item count: {remaining_count}")
            
        if remaining_count > 0:
            print(f"Warning: {remaining_count} items still remain in the table after deletion attempt")
            return (True, initial_count, deleted_count, remaining_count)
    except Exception as e:
        print(f"Error verifying deletion in table {table_name}: {str(e)}")
        return (False, initial_count, deleted_count, 0)
        
    return (True, initial_count, deleted_count, 0)

def main():
    parser = argparse.ArgumentParser(description='Delete all entries from DynamoDB table.')
    parser.add_argument('-t', '--table_name', type=str, required=True, help='Name of the table to delete entries from.')
    parser.add_argument('-e', '--aws_endpoint', type=str, help='AWS endpoint URL (optional).')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output.')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt.')
    parser.add_argument('--pk', type=str, default='id', help='Primary key name. Defaults to "id" if not specified.')
    args = parser.parse_args()
    
    if not args.force:
        print(f"WARNING: You are about to delete ALL items from the table '{args.table_name}'")
        confirmation = input("Are you sure you want to proceed? (yes/no): ")
        if confirmation.lower() != "yes":
            print("Operation cancelled.")
            sys.exit(0)
    
    success, initial_count, deleted_count, remaining_count = delete_table_entries(
        args.table_name, 
        args.aws_endpoint,
        args.verbose,
        args.pk
    )
    
    if success:
        if remaining_count > 0:
            print(f"Warning: Attempted to wipe {args.table_name}. {deleted_count} items deleted, but {remaining_count} items still remain.")
        else:
            print(f"Success: Wiped {args.table_name}. {deleted_count} items deleted.")
    else:
        print(f"Error: Failed to wipe {args.table_name} completely. Only {deleted_count} of {initial_count} items were deleted.")
        sys.exit(1)

if __name__ == '__main__':
    main() 