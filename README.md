# AWS Utils 👨‍💻
This project provides several utilities for working with AWS. Each utility is a standalone Python file that uses argparse for command-line argument parsing.

- Import CSV into dynamoDB 
- Wip out Dynamodb table
- Scan infinite Dynamodb table items

## Installation
- clone the repository
- create virtual environment `python -m venv .venv`
- switch to virtual environment `source .venv/bin/activate`
- install the required packages with `pip install -r requirements.txt`
- when done, exit virtual environment with typing `deactivate`

## Example Usage
__wipe_table.py__
This utility wipes all the items from a DynamoDB table.
```
python wipe_table.py <table_name> [--aws_endpoint <aws_endpoint>]
```
__import_csv.py__
This utility imports csv file (key, value) into selected DynamoDB table.
```
python import_csv.py --table=my_table --file=path_to_my_file.csv

# optional parameters with default values overriden
python import_csv.py --table=my_table --file=path_to_my_file.csv --profile=my-profile --region=eu-west-2
```

**Arguments**
- `table_name:` The name of the DynamoDB table to wipe.
- `aws_endpoint:` (optional) The endpoint to use for connecting to DynamoDB. If not specified, the default endpoint for the region will be used.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
