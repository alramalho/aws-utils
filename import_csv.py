import sys
import csv
import boto3
import argparse
from botocore.exceptions import ClientError

def filter_and_import_csv_to_dynamodb(table_name, csv_file, columns_to_keep=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    items = []
    with open(csv_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        
        # If columns_to_keep is None, use all columns
        if columns_to_keep is None:
            columns_to_keep = csv_reader.fieldnames
        else:
            # Validate that all specified columns exist in the CSV
            all_columns = csv_reader.fieldnames
            for column in columns_to_keep:
                if column not in all_columns:
                    raise ValueError(f"Column '{column}' not found in CSV. Available columns: {', '.join(all_columns)}")
        
        for row in csv_reader:
            filtered_row = {col: row[col] for col in columns_to_keep}
            items.append(filtered_row)

    # Batch write items to DynamoDB
    with table.batch_writer() as batch:
        for item in items:
            try:
                batch.put_item(Item=item)
            except ClientError as e:
                print(f"Error importing item: {item}")
                print(e.response['Error']['Message'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import filtered CSV data to DynamoDB")
    parser.add_argument("-t", "--table", type=str, help="Name of the DynamoDB table", required=True)
    parser.add_argument("-f", "--file", type=str, help="Path to the CSV file", required=True)
    parser.add_argument("-k", "--keep", type=str, nargs='*', help="Columns to keep (space-separated). If not specified, all columns will be kept.", default=None)
    args = parser.parse_args()

    filter_and_import_csv_to_dynamodb(args.table, args.file, args.keep)
    print("Filtered import complete!")