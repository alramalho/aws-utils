import argparse
import csv

import boto3

# ... [Previous Code]

def load_data_from_csv(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        items = [row for row in reader]
    return items

def insert_from_csv(table_name, csv_file, aws_endpoint=None):
    items = load_data_from_csv(csv_file)
    for item in items:
        create_table_entry(table_name, item, aws_endpoint)


def create_table_entry(table_name, item_data, aws_endpoint=None):
    dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint) if aws_endpoint else boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.put_item(Item=item_data)
    return response

def read_table_entry(table_name, key_data, aws_endpoint=None):
    dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint) if aws_endpoint else boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key_data)
    return response.get('Item')

def update_table_entry(table_name, key_data, update_data, aws_endpoint=None):
    dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint) if aws_endpoint else boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.update_item(Key=key_data, AttributeUpdates=update_data)
    return response

def delete_table_entry(table_name, key_data, aws_endpoint=None):
    dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint) if aws_endpoint else boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.delete_item(Key=key_data)
    return response

def delete_all_table_entries(table_name, aws_endpoint=None):
    if aws_endpoint:
        dynamodb = boto3.resource('dynamodb', endpoint_url=aws_endpoint)
        client = boto3.client('dynamodb', endpoint_url=aws_endpoint)
    else:
        dynamodb = boto3.resource('dynamodb')
        client = boto3.client('dynamodb')
    
    table = dynamodb.Table(table_name)
    response = client.describe_table(TableName=table_name)
    key_name = response['Table']['KeySchema'][0]['AttributeName']

    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={key_name: each[key_name]})

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CRUD operations for DynamoDB table.')
    parser.add_argument('operation', choices=['create', 'read', 'update', 'delete', 'delete_all', 'insert'], help='CRUD operation to perform.')
    parser.add_argument('table_name', type=str, help='Name of the table.')
    parser.add_argument('--item_data', type=dict, help='Data for the item (for create operation).')
    parser.add_argument('--key_data', type=dict, help='Primary key data for the item (for read, update, delete operations).')
    parser.add_argument('--update_data', type=dict, help='Data to update for the item (for update operation).')
    parser.add_argument('-e', '--aws_endpoint', type=str, help='AWS endpoint URL (optional).')
    parser.add_argument('--csv_file', type=str, help='Path to the CSV file to load data from (for create operation).')
    
    args = parser.parse_args()
    
    
    if args.operation == 'create':
        create_table_entry(args.table_name, args.item_data, args.aws_endpoint)
    elif args.operation == 'read':
        print(read_table_entry(args.table_name, args.key_data, args.aws_endpoint))
    elif args.operation == 'update':
        update_table_entry(args.table_name, args.key_data, args.update_data, args.aws_endpoint)
    elif args.operation == 'delete':
        delete_table_entry(args.table_name, args.key_data, args.aws_endpoint)
    elif args.operation == 'delete_all':
        delete_all_table_entries(args.table_name, args.aws_endpoint)
    elif args.operation == 'insert':
        if args.csv_file:
            insert_from_csv(args.table_name, args.csv_file, args.aws_endpoint)
        else:
            print("Please provide --csv_file for the insert operation.")
        
    print('Operation completed.')

