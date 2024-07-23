import boto3
import argparse
from botocore.exceptions import ClientError

def remove_column_from_dynamodb(table_name, column_to_remove):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Scan the table to get all items
    response = table.scan()
    items = response['Items']

    # Handle pagination if there are more items
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    print(f"Found {len(items)} items in the table.")

    # Remove the specified column from each item and update the table
    with table.batch_writer() as batch:
        for item in items:
            if column_to_remove in item:
                del item[column_to_remove]
                try:
                    batch.put_item(Item=item)
                    print(f"Updated item: {item['id'] if 'id' in item else item}")
                except ClientError as e:
                    print(f"Error updating item: {item}")
                    print(e.response['Error']['Message'])
            else:
                print(f"Column '{column_to_remove}' not found in item: {item['id'] if 'id' in item else item}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove a column from all items in a DynamoDB table")
    parser.add_argument("-t", "--table", type=str, help="Name of the DynamoDB table", required=True)
    parser.add_argument("-c", "--column", type=str, help="Name of the column to remove", required=True)
    args = parser.parse_args()

    remove_column_from_dynamodb(args.table, args.column)
    print("Column removal complete!")