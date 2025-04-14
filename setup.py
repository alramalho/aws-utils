from setuptools import setup, find_packages

setup(
    name="aws-utils",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "boto3>=1.26.0",
        "argparse",
        "colorama",
    ],
    entry_points={
        "console_scripts": [
            "aws-scan-table=aws_utils.scan_table:main",
            "aws-wipe-table=aws_utils.wipe_table:main",
            "aws-crud-table=aws_utils.crud_table:main",
            "aws-remove-column=aws_utils.remove_dynamo_columnn:main",
            "aws-migrate-table=aws_utils.migrate_table_data:main",
            "aws-import-csv=aws_utils.import_csv:main",
            "aws-update-item=aws_utils.update_item_key_value:main",
            "aws-rename-column=aws_utils.rename_column:main",
            "aws-export-csv=aws_utils.export_to_csv:main",
            "aws-import-json=aws_utils.import_json:main",
            "aws-mongo-to-dynamo=aws_utils.mongo_to_dynamo:main",
        ],
    },
    author="alramalho",
    author_email="your.email@example.com",
    description="A collection of AWS utilities for DynamoDB operations",
    keywords="aws, dynamodb, utils",
    url="https://github.com/yourusername/aws-utils",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
) 