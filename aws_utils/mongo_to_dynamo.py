import argparse
import subprocess
import os
import sys
import json
import importlib.util
from pathlib import Path
from colorama import Fore, Style

def ensure_dir(directory):
    """Ensure a directory exists."""
    Path(directory).mkdir(parents=True, exist_ok=True)

def mongo_to_dynamo_table(mongo_uri, mongo_db, mongo_collection, dynamo_table, 
                           temp_dir="./tmp/mongo_export", force=False):
    """
    Export a MongoDB collection to a DynamoDB table.
    
    Args:
        mongo_uri (str): MongoDB connection URI
        mongo_db (str): MongoDB database name
        mongo_collection (str): MongoDB collection name
        dynamo_table (str): DynamoDB table name
        temp_dir (str): Directory to store temporary files
        force (bool): Skip confirmation prompt if True
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Confirm before proceeding
    if not force:
        print(f"This will export data from MongoDB collection '{mongo_collection}' in database '{mongo_db}'")
        print(f"And import it into DynamoDB table '{dynamo_table}'")
        confirm = input("Are you sure you want to continue? (yes/no): ")
        if confirm.lower() != "yes":
            print("Operation cancelled.")
            return False
    
    # Create temp directory if it doesn't exist
    ensure_dir(temp_dir)
    
    # Set JSON file paths
    mongo_json_file = os.path.join(temp_dir, f"{mongo_collection}.jsonl")
    dynamo_json_file = os.path.join(temp_dir, f"{mongo_collection}_dynamo.jsonl")
    
    print(f"{Fore.YELLOW}[STEP]{Style.RESET_ALL} Exporting MongoDB collection '{mongo_collection}' from database '{mongo_db}' to JSONL...")
    
    # Export MongoDB collection to JSONL (each document on a new line)
    try:
        subprocess.run([
            "mongoexport", 
            f"--uri={mongo_uri}", 
            f"--db={mongo_db}", 
            f"--collection={mongo_collection}", 
            f"--out={mongo_json_file}", 
            "--authenticationDatabase=admin"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} Failed to export MongoDB collection: {e}")
        return False
    except FileNotFoundError:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} mongoexport command not found. Please install MongoDB tools.")
        return False
    
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Successfully exported to {mongo_json_file}")
    
    print(f"{Fore.YELLOW}[STEP]{Style.RESET_ALL} Converting MongoDB IDs to DynamoDB format...")
    
    # Check if the mongo_to_dynamo_object command is available
    # First try as a direct command
    try:
        # Get the current directory
        current_dir = os.getcwd()
        # Change to mongo_to_dynamo_object directory
        mongo_converter_dir = os.path.join(current_dir, "mongo_to_dynamo_object")
        
        # Check if cargo and rust are available
        if not os.path.exists(mongo_converter_dir):
            print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} mongo_to_dynamo_object directory not found at: {mongo_converter_dir}")
            return False
            
        # Run the cargo command
        os.chdir(mongo_converter_dir)
        subprocess.run([
            "cargo", "run", "--", 
            "--input", f"../{mongo_json_file}", 
            "--output", f"../{dynamo_json_file}"
        ], check=True)
        # Change back to original directory
        os.chdir(current_dir)
    except subprocess.CalledProcessError as e:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} Failed to convert MongoDB IDs: {e}")
        # Change back to original directory if an error occurred
        if 'current_dir' in locals():
            os.chdir(current_dir)
        return False
    except FileNotFoundError:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} cargo command not found. Please install Rust and Cargo.")
        # Change back to original directory if an error occurred
        if 'current_dir' in locals():
            os.chdir(current_dir)
        return False
        
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Successfully converted MongoDB IDs to DynamoDB format")
    
    print(f"{Fore.YELLOW}[STEP]{Style.RESET_ALL} Importing data to DynamoDB table '{dynamo_table}'...")
    
    # Import the import_json module from our package
    try:
        from aws_utils.import_json import import_json
        
        # Call the import_json function
        import_json(
            table_name=dynamo_table,
            file_path=dynamo_json_file,
            jsonl=True
        )
    except Exception as e:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} Failed to import data to DynamoDB: {e}")
        return False
    
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Successfully imported data from MongoDB to DynamoDB!")
    print("Temporary files:")
    print(f"- MongoDB export: {mongo_json_file}")
    print(f"- DynamoDB import: {dynamo_json_file}")
    
    return True

def full_migration(mongo_uri, mongo_db, target_env="dev", force=False):
    """
    Perform a full migration of all collections from MongoDB to DynamoDB.
    
    Args:
        mongo_uri (str): MongoDB connection URI
        mongo_db (str): MongoDB database name
        target_env (str): Environment suffix for DynamoDB tables (prod/dev)
        force (bool): Skip confirmation prompts if True
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Collections to migrate
    collections = [
        ('users', f'tracking_software_users_{target_env}'),
        ('activities', f'tracking_software_activities_{target_env}'),
        ('activity_entries', f'tracking_software_activity_entries_{target_env}'),
        ('metrics', f'tracking_software_metrics_{target_env}'),
        ('metric_entries', f'tracking_software_metric_entries_{target_env}'),
        ('plans', f'tracking_software_plans_{target_env}'),
        ('plan_groups', f'tracking_software_plan_groups_{target_env}'),
        ('friend_requests', f'tracking_software_friend_requests_{target_env}'),
        ('plan_invitations', f'tracking_software_plan_invitations_{target_env}'),
        ('mood_reports', f'tracking_software_mood_reports_{target_env}'),
        ('messages', f'tracking_software_messages_{target_env}'),
        ('notifications', f'tracking_software_notifications_{target_env}')
    ]
    
    # Confirm before proceeding
    if not force:
        print(f"This will migrate the following collections from MongoDB database '{mongo_db}'")
        print(f"to DynamoDB tables with '{target_env}' suffix:")
        for mongo_col, dynamo_table in collections:
            print(f"  - {mongo_col} → {dynamo_table}")
        
        confirm = input("Are you sure you want to continue? (yes/no): ")
        if confirm.lower() != "yes":
            print("Migration cancelled.")
            return False
    
    # Track success
    success_count = 0
    failed_collections = []
    
    # Migrate each collection
    for mongo_col, dynamo_table in collections:
        print(f"\n{Fore.YELLOW}[MIGRATING]{Style.RESET_ALL} {mongo_col} → {dynamo_table}")
        
        success = mongo_to_dynamo_table(
            mongo_uri=mongo_uri,
            mongo_db=mongo_db,
            mongo_collection=mongo_col,
            dynamo_table=dynamo_table,
            force=True  # Skip individual confirmations
        )
        
        if success:
            success_count += 1
        else:
            failed_collections.append(mongo_col)
    
    # Print summary
    print("\n" + "="*50)
    print(f"Migration Summary ({success_count}/{len(collections)} successful):")
    
    if failed_collections:
        print(f"{Fore.RED}[FAILED COLLECTIONS]{Style.RESET_ALL}")
        for coll in failed_collections:
            print(f"  - {coll}")
    
    if success_count == len(collections):
        print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} All collections migrated successfully!")
        return True
    else:
        print(f"{Fore.YELLOW}[PARTIAL SUCCESS]{Style.RESET_ALL} {success_count} of {len(collections)} collections migrated.")
        return False

def main():
    parser = argparse.ArgumentParser(description='MongoDB to DynamoDB migration utilities.')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Single table migration command
    table_parser = subparsers.add_parser('table', help='Migrate a single MongoDB collection to DynamoDB')
    table_parser.add_argument('--mongo-uri', required=True, help='MongoDB connection URI')
    table_parser.add_argument('--mongo-db', required=True, help='MongoDB database name')
    table_parser.add_argument('--mongo-collection', required=True, help='MongoDB collection name')
    table_parser.add_argument('--dynamo-table', required=True, help='DynamoDB table name')
    table_parser.add_argument('--temp-dir', default='./tmp/mongo_export', help='Directory for temporary files')
    table_parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    
    # Full migration command
    full_parser = subparsers.add_parser('full', help='Perform full migration of all collections')
    full_parser.add_argument('--mongo-uri', required=True, help='MongoDB connection URI')
    full_parser.add_argument('--mongo-db', required=True, help='MongoDB database name')
    full_parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Target environment (dev/prod)')
    full_parser.add_argument('--force', action='store_true', help='Skip confirmation prompts')
    
    args = parser.parse_args()
    
    if args.command == 'table':
        mongo_to_dynamo_table(
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            mongo_collection=args.mongo_collection,
            dynamo_table=args.dynamo_table,
            temp_dir=args.temp_dir,
            force=args.force
        )
    elif args.command == 'full':
        full_migration(
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            target_env=args.env,
            force=args.force
        )
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 