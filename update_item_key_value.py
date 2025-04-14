import argparse
import boto3

def update_item_key_value(
    table_name, 
    primary_key_name, 
    primary_key_value,
    key_name,
    old_value,
    new_value,
    aws_endpoint=None
):
    """
    Update a specific key value in a DynamoDB item.
    
    Args:
        table_name (str): Name of the DynamoDB table
        primary_key_name (str): Name of the primary key (e.g., 'id')
        primary_key_value (str): Value of the primary key to identify the item
        key_name (str): Name of the key to update
        old_value (str): Current value of the key (for verification)
        new_value (str): New value to set
        aws_endpoint (str, optional): AWS endpoint URL. Defaults to None.
    """
    # Connect to DynamoDB
    if aws_endpoint:
        dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint)
    else:
        dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table(table_name)
    
    # First, get the item to verify it exists and check the old value
    response = table.get_item(Key={primary_key_name: primary_key_value})
    
    if 'Item' not in response:
        print(f"❌ Error: Item with {primary_key_name}={primary_key_value} not found in {table_name}")
        return False
    
    item = response['Item']
    
    # Check if the key exists
    if key_name not in item:
        print(f"❌ Error: Key '{key_name}' not found in item")
        return False
    
    # Check if the old value matches (if old_value was provided)
    if old_value is not None and str(item[key_name]) != str(old_value):
        print(f"❌ Error: Current value of '{key_name}' is '{item[key_name]}', not '{old_value}'")
        return False
    
    # Update the item
    response = table.update_item(
        Key={primary_key_name: primary_key_value},
        UpdateExpression=f"SET {key_name} = :new_val",
        ExpressionAttributeValues={':new_val': new_value},
        ReturnValues="UPDATED_NEW"
    )
    
    print(f"✅ Successfully updated {key_name} from '{item[key_name]}' to '{new_value}'")
    print(f"Updated values: {response.get('Attributes', {})}")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update a specific key value in a DynamoDB item')
    parser.add_argument('-t', '--table', required=True, help='DynamoDB table name')
    parser.add_argument('-p', '--primary_key_name', required=True, help='Primary key name (e.g. "id")')
    parser.add_argument('-v', '--primary_key_value', required=True, help='Primary key value')
    parser.add_argument('-k', '--key_name', required=True, help='Name of the key to update')
    parser.add_argument('-o', '--old_value', help='Current value of the key (for verification, optional)')
    parser.add_argument('-n', '--new_value', required=True, help='New value to set')
    parser.add_argument('-e', '--aws_endpoint', help='AWS endpoint URL (optional)')
    
    args = parser.parse_args()
    
    update_item_key_value(
        args.table,
        args.primary_key_name,
        args.primary_key_value,
        args.key_name,
        args.old_value,
        args.new_value,
        args.aws_endpoint
    ) 