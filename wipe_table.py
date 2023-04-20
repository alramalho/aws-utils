import boto3
import argparse

def delete_table_entries(table_name, aws_endpoint=None):
    if aws_endpoint:
        dynamodb = boto3.resource('dynamodb')
    else:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key=each)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Delete all entries from DynamoDB table.')
    parser.add_argument('table_name', type=str, help='Name of the table to delete entries from.')
    parser.add_argument('--aws_endpoint', type=str, help='AWS endpoint URL (optional).')
    args = parser.parse_args()
    delete_table_entries(args.table_name, args.aws_endpoint)
    print('Done ✅')
