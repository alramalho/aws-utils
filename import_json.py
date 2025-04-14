import sys
import json
import boto3
import argparse
from botocore.exceptions import ClientError

def filter_and_import_json_to_dynamodb(table_name, json_file, keys_to_keep=None, is_jsonl=False):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Load JSON data
    items = []
    with open(json_file, 'r') as file:
        if is_jsonl:
            # Process JSONL format (each line is a JSON object)
            for line_num, line in enumerate(file, 1):
                try:
                    if line.strip():  # Skip empty lines
                        item = json.loads(line)
                        items.append(item)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON at line {line_num}: {e}")
                    print(f"Problematic line: {line.strip()}")
                    sys.exit(1)
        else:
            # Process regular JSON format
            try:
                data = json.load(file)
                # Handle different JSON structures (single object or array of objects)
                if isinstance(data, dict):
                    items = [data]
                elif isinstance(data, list):
                    items = data
                else:
                    print(f"Unsupported JSON structure. Expected object or array, got {type(data).__name__}")
                    sys.exit(1)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                sys.exit(1)
    
    if not items:
        print("Warning: No valid JSON items found in the file.")
        sys.exit(1)
    
    # Filter keys if specified
    filtered_items = []
    for item in items:
        if keys_to_keep is None:
            # Keep all keys
            filtered_items.append(item)
        else:
            # Validate that specified keys exist in at least one item
            if filtered_items == []:  # Only check the first time
                all_keys = set(item.keys())
                for key in keys_to_keep:
                    if key not in all_keys:
                        print(f"Warning: Key '{key}' not found in the first JSON item. Available keys: {', '.join(all_keys)}")
            
            # Filter the item
            filtered_item = {key: item.get(key) for key in keys_to_keep if key in item}
            filtered_items.append(filtered_item)
    
    # Batch write items to DynamoDB
    with table.batch_writer() as batch:
        for item in filtered_items:
            try:
                batch.put_item(Item=item)
            except ClientError as e:
                print(f"Error importing item: {item}")
                print(e.response['Error']['Message'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import filtered JSON data to DynamoDB")
    parser.add_argument("-t", "--table", type=str, help="Name of the DynamoDB table", required=True)
    parser.add_argument("-f", "--file", type=str, help="Path to the JSON file", required=True)
    parser.add_argument("-k", "--keep", type=str, nargs='*', help="Keys to keep (space-separated). If not specified, all keys will be kept.", default=None)
    parser.add_argument("--jsonl", action="store_true", help="Treat input as JSONL format (one JSON object per line)")
    args = parser.parse_args()

    filter_and_import_json_to_dynamodb(args.table, args.file, args.keep, args.jsonl)
    print("Filtered import complete!") 