import argparse
import boto3

def update_item_key_value(
    table_name, 
    pk_name, 
    pk_value,
    key_name,
    new_value,
    old_value=None,
    aws_endpoint=None,
    value_type=None
):
    """
    Update a specific key value in a DynamoDB item.
    
    Args:
        table_name (str): Name of the DynamoDB table
        pk_name (str): Name of the primary key (e.g., 'id')
        pk_value (str): Value of the primary key to identify the item
        key_name (str): Name of the key to update
        new_value (str): New value to set
        old_value (str, optional): Current value of the key (for verification). Defaults to None.
        aws_endpoint (str, optional): AWS endpoint URL. Defaults to None.
        value_type (str, optional): DynamoDB type (S, N, BOOL, etc). Defaults to None (auto-detect).
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Connect to DynamoDB
    if aws_endpoint:
        dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint)
        client = boto3.client('dynamodb', endpoint_url=aws_endpoint)
    else:
        dynamodb = boto3.resource('dynamodb')
        client = boto3.client('dynamodb')
    
    # For the boto3 resource API
    table = dynamodb.Table(table_name)
    
    # First, get the item to verify it exists
    try:
        response = table.get_item(Key={pk_name: pk_value})
    except Exception as e:
        print(f"❌ Error accessing item: {str(e)}")
        return False
    
    if 'Item' not in response:
        print(f"❌ Error: Item with {pk_name}={pk_value} not found in {table_name}")
        return False
    
    item = response['Item']
    
    # Check if the old value matches (if old_value was provided)
    if old_value is not None and key_name in item and str(item[key_name]) != str(old_value):
        print(f"❌ Error: Current value of '{key_name}' is '{item[key_name]}', not '{old_value}'")
        return False
    
    # If value_type is specified, we'll use the DynamoDB client API
    if value_type:
        # Convert the new value to the appropriate DynamoDB format
        if value_type == 'N':
            typed_value = {'N': str(new_value)}
        elif value_type == 'S':
            typed_value = {'S': str(new_value)}
        elif value_type == 'BOOL':
            typed_value = {'BOOL': new_value.lower() in ('true', 't', 'yes', 'y', '1')}
        else:
            typed_value = {value_type: str(new_value)}
        
        try:
            # Use the client API to update with typed value
            response = client.update_item(
                TableName=table_name,
                Key={pk_name: {'S': str(pk_value)}},
                UpdateExpression=f"SET {key_name} = :new_val",
                ExpressionAttributeValues={':new_val': typed_value},
                ReturnValues="UPDATED_NEW"
            )
            current_value = item.get(key_name, "None")
            print(f"✅ Successfully updated {key_name} from '{current_value}' to '{new_value}' with type {value_type}")
            return True
        except Exception as e:
            print(f"❌ Error updating item: {str(e)}")
            return False
    else:
        # Use the simpler resource API
        try:
            response = table.update_item(
                Key={pk_name: pk_value},
                UpdateExpression=f"SET {key_name} = :new_val",
                ExpressionAttributeValues={':new_val': new_value},
                ReturnValues="UPDATED_NEW"
            )
            current_value = item.get(key_name, "None")
            print(f"✅ Successfully updated {key_name} from '{current_value}' to '{new_value}'")
            print(f"Updated values: {response.get('Attributes', {})}")
            return True
        except Exception as e:
            print(f"❌ Error updating item: {str(e)}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Update a specific key value in a DynamoDB item')
    parser.add_argument('-t', '--table', required=True, help='DynamoDB table name')
    parser.add_argument('-p', '--primary_key_name', required=True, help='Primary key name (e.g. "id")')
    parser.add_argument('-v', '--primary_key_value', required=True, help='Primary key value')
    parser.add_argument('-k', '--key_name', required=True, help='Name of the key to update')
    parser.add_argument('-o', '--old_value', help='Current value of the key (for verification, optional)')
    parser.add_argument('-n', '--new_value', required=True, help='New value to set')
    parser.add_argument('-e', '--aws_endpoint', help='AWS endpoint URL (optional)')
    parser.add_argument('-T', '--type', choices=['S', 'N', 'BOOL', 'M', 'L', 'SS', 'NS', 'BS'],
                       help='DynamoDB attribute type (S=string, N=number, etc.)')
    
    args = parser.parse_args()
    
    update_item_key_value(
        table_name=args.table,
        pk_name=args.primary_key_name,
        pk_value=args.primary_key_value,
        key_name=args.key_name,
        new_value=args.new_value,
        old_value=args.old_value,
        aws_endpoint=args.aws_endpoint,
        value_type=args.type
    )

if __name__ == "__main__":
    main() 