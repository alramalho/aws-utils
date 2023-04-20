import sys
import csv
import boto3
import argparse

csv.field_size_limit(sys.maxsize)

dynamodb = boto3.resource('dynamodb')

if __name__ == "__main__":
    # Parser setup
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--file", type=str, help="path of the file to upload", required=True)
    parser.add_argument(
        "-t", "--table", type=str, help="table name where to upload to", required=True)
    args = parser.parse_args()
    filename = args.file
    tableName = args.table

    # CSV Reading
    csvfile = open(filename)
    rows = csv.DictReader(csvfile)
    
    # DynamoDB Writing
    table = dynamodb.Table(tableName)
    with table.batch_writer() as batch:
        for row in rows:
            try:
                batch.put_item(
                    Item=row
                )
            except Exception as e:
                print("Row: ", (row['id'] if id in row else "No id"))
                print("Error: ", e)
                continue

    print("Done")
