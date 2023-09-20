import sys
import csv
import boto3
import argparse

dynamodb = boto3.resource('dynamodb')

def export_dynamodb_to_csv(table_name, output_file):
    # DynamoDB Reading
    table = dynamodb.Table(table_name)
    
    # Starting with the first page of results
    response = table.scan()
    items = response.get('Items')
    
    # Handle paginated results
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items'))

    # CSV Writing
    if items:
        keys = items[0].keys()
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            for item in items:
                writer.writerow(item)

if __name__ == "__main__":
    # Parser setup
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--table", type=str, help="name of the table to export from", required=True)
    parser.add_argument(
        "-o", "--output", type=str, help="path of the CSV file to write to", required=True)
    args = parser.parse_args()
    
    export_dynamodb_to_csv(args.table, args.output)
    print("Export complete!")
