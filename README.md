# DyanamoDB Utils 👨‍💻
This project provides several utilities for working with DynamoDB. Each utility is a standalone Python file that uses argparse for command-line argument parsing.

- Import CSV into dynamoDB 
- Wip out Dynamodb table

## Installation
Clone the repository.
Install the required packages with pip install -r requirements.txt.

## Example Usage
wipe_table.py
This utility wipes all the items from a DynamoDB table.

```
python wipe_table.py --table_name <table_name> [--aws_endpoint <aws_endpoint>]
```

**Arguments**
- `table_name:` The name of the DynamoDB table to wipe.
- `aws_endpoint:` (optional) The endpoint to use for connecting to DynamoDB. If not specified, the default endpoint for the region will be used.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
