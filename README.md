
<img src="./header.png" style="width: 100%"/>

# Minimal python CSV to DynamoDB Importer

This open source tool allows you to easily import CSV files into Amazon DynamoDB tables. The code is written in Python and is under 40 lines, making it easy to read and modify.

## Disclaimer

Please note that this tool was tested only with CSV exports directly from AWS. If you encounter any issues with other CSV formats, please feel free to contribute by submitting a pull request with the necessary modifications.
## Usage

### 1 requirements
Install the requirements
```bash
pip install -r requirements.txt
```

### 2 run

Run the script `run.py` to import CSV files into DynamoDB tables. It takes two required arguments:

- `--file` or `-f`: The path to the CSV file you want to import.
- `--table` or `-t`: The name of the DynamoDB table you want to import the data into.

You can run the script with the following command:

```bsh
git clone https://github.com/alramalho/csv-into-dynamodb

python csv-into-dynamodb/run.py --file file.csv --table table_name
```

If you need help, you can use the `--help` or `-h` flag:

```python

python run.py --help
```

This will print the help message, which includes information on the required and optional arguments, as well as a brief description of what the script does.
Contribution

This tool will remain open source forever. Please feel free to add special cases or modify the code as necessary and submit a pull request. Your contributions are greatly appreciated!
