#!/usr/bin/env python3

import boto3
import argparse
from typing import Dict, Any
import time

def rename_column(
    table_name: str,
    old_column_name: str,
    new_column_name: str,
    region: str = "us-east-1"
) -> None:
    """
    Rename a column in a DynamoDB table by copying the value to a new column name
    and then removing the old column.

    Args:
        table_name (str): Name of the DynamoDB table
        old_column_name (str): Name of the column to rename
        new_column_name (str): New name for the column
        region (str): AWS region name
    """
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)

    # Get the table's key schema
    table_description = table.meta.client.describe_table(TableName=table_name)
    key_schema = table_description['Table']['KeySchema']
    key_attributes = [key['AttributeName'] for key in key_schema]

    # Scan the table to get all items
    response = table.scan()
    items = response.get('Items', [])

    # Continue scanning if we have more items
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    print(f"Found {len(items)} items to process")
    print(f"Using key attributes: {key_attributes}")

    # Update each item
    for item in items:
        if old_column_name in item:
            # Create update expression to set new column and remove old one
            update_expression = f"SET #{new_column_name} = :old_value REMOVE #{old_column_name}"
            
            # Create expression attribute names and values
            expression_attribute_names = {
                f"#{new_column_name}": new_column_name,
                f"#{old_column_name}": old_column_name
            }
            expression_attribute_values = {
                ":old_value": item[old_column_name]
            }

            # Create the key dictionary using only the primary key attributes
            key_dict = {k: item[k] for k in key_attributes if k in item}

            try:
                table.update_item(
                    Key=key_dict,
                    UpdateExpression=update_expression,
                    ExpressionAttributeNames=expression_attribute_names,
                    ExpressionAttributeValues=expression_attribute_values
                )
                print(f"Updated item with key: {key_dict}")
            except Exception as e:
                print(f"Error updating item: {e}")
                print(f"Item that caused error: {item}")
            
            # Add a small delay to avoid throttling
            time.sleep(0.1)

def main():
    parser = argparse.ArgumentParser(description='Rename a column in a DynamoDB table')
    parser.add_argument('--table-name', required=True, help='Name of the DynamoDB table')
    parser.add_argument('--old-column-name', required=True, help='Name of the column to rename')
    parser.add_argument('--new-column-name', required=True, help='New name for the column')
    parser.add_argument('--region', default='us-east-1', help='AWS region name (default: us-east-1)')

    args = parser.parse_args()

    rename_column(
        table_name=args.table_name,
        old_column_name=args.old_column_name,
        new_column_name=args.new_column_name,
        region=args.region
    )

if __name__ == '__main__':
    main() 