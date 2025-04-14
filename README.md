# AWS Utils

A collection of command-line utilities for working with AWS DynamoDB.

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/aws-utils.git
cd aws-utils

# Install in development mode
pip install -e .

# Or install directly
pip install .
```

## Available Commands

After installation, the following commands will be available in your terminal:

- `aws-scan-table` - Scan and view items in a DynamoDB table
- `aws-wipe-table` - Delete all items from a DynamoDB table
- `aws-crud-table` - Perform CRUD operations on a DynamoDB table
- `aws-remove-column` - Remove a column from all items in a DynamoDB table
- `aws-migrate-table` - Migrate data between DynamoDB tables
- `aws-import-csv` - Import data from a CSV file into a DynamoDB table
- `aws-update-item` - Update a specific item's field value in a DynamoDB table
- `aws-rename-column` - Rename a column in a DynamoDB table
- `aws-export-csv` - Export DynamoDB table data to a CSV file
- `aws-import-json` - Import data from a JSON file into a DynamoDB table
- `aws-mongo-to-dynamo` - MongoDB to DynamoDB migration utilities

## Example Usage

```bash
# Scan a table
aws-scan-table -t my-table -m 10 -c user_type

# Wipe a table (delete all items)
aws-wipe-table -t my-table --force

# Export a table to CSV
aws-export-csv -t my-table -o output.csv

# Import data from CSV
aws-import-csv -t my-table -f data.csv

# Migrate a single MongoDB collection to DynamoDB
aws-mongo-to-dynamo table --mongo-uri "mongodb://localhost:27017" --mongo-db "mydb" --mongo-collection "users" --dynamo-table "Users_dev"

# Perform full MongoDB to DynamoDB migration
aws-mongo-to-dynamo full --mongo-uri "mongodb://localhost:27017" --mongo-db "mydb" --env dev
```

Run any command with `-h` to see all available options.

## AWS Configuration

These utilities use the boto3 library, which requires AWS credentials to be configured. 
You can configure your credentials using:

```bash
aws configure
```

Or by setting environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-west-2"
```

## Local Development with DynamoDB Local

Most commands support a `-e` or `--aws_endpoint` parameter to work with a local DynamoDB instance:

```bash
aws-scan-table -t my-table -e http://localhost:8000
```
