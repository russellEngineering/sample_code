import os
import pandas as pd
from datetime import datetime, timedelta
import json
import shutil
from logging_config import setup_logging
import logging
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from pymongo.server_api import ServerApi
import urllib.parse


def expand_env_vars(config):
    """
    Recursively expand environment variables in all string fields of a config dictionary.
    """
    if isinstance(config, dict):
        return {key: expand_env_vars(value) for key, value in config.items()}
    elif isinstance(config, list):
        return [expand_env_vars(item) for item in config]
    elif isinstance(config, str):
        return os.path.expandvars(config)
    else:
        return config


# logging
setup_logging()
logger = logging.getLogger(__name__)


# read config file for senders
with open('config.json', 'r') as file:
    # Load JSON leeds
    config = json.load(file)

# connect to db
config = expand_env_vars(config)
encoded_password = urllib.parse.quote_plus(config["database"]["password"])
username = config["database"]["username"]
server_ip = config["database"]["host_ip"]
server_port = config["database"]["port"]
database_name = config["database"]["database_name"]
connection_string = f"mongodb+srv://{username}:{encoded_password}@usercluster.1ud6a.mongodb.net/?retryWrites=true&w=majority&appName=userCluster"
# connection_string = f'mongodb://{username}:{encoded_password}@{server_ip}:{server_port}/{database_name}'

try:
    client = MongoClient(connection_string, server_api=ServerApi('1'))
    client.admin.command('ping')
    logger.info("Connected to Leeds DB")

except ConnectionFailure as e:
    logger.fatal(f"Connection to MongoDB failed: {e}")

leeds_db = client[database_name]
leeds_collection_name = config["database"]["surface_leeds_collection"]
leeds_collection = leeds_db[leeds_collection_name]
domains_collection = leeds_db[config["database"]["steel_domain_stats_collection"]]

read_dir = "leads"
processed_dir = "processed_leads"


days_between_contact = config["Email Settings"]["days_between_contact"]
engagement_date_key = config["Campaign Fields"]["engagement_date_key"]
copy_stage_key = config["Campaign Fields"]["copy_stage_key"]
client_name_key = config["Csv Fields"]["client_name_key"]
client_email_key = config["Csv Fields"]["client_email_key"]

# Ensure the processed directory exists
os.makedirs(processed_dir, exist_ok=True)


def populate_unique_domains(in_leads_df):
    # Extract the 'domains' column and get unique values

    unique_domains = in_leads_df['Email'].str.split('@').str[1].unique()

    # Create a unique index on the 'domain' field
    domains_collection.create_index("domain", unique=True)

    # Insert unique domains into the collection
    for domain in unique_domains:
        document = {"domain": domain, "bounced": 0}
        try:
            domains_collection.insert_one(document)  # Attempt to insert the document
            logger.info(f"Domain {domain} inserted successfully!")
        except DuplicateKeyError:
            print(f"Duplicate domain skipped: {domain}")


# Process files
for filename in os.listdir(read_dir):
    if filename.endswith(".csv"):
        file_path = os.path.join(read_dir, filename)

        try:
            # Read CSV into a DataFrame
            leeds_df = pd.read_csv(file_path, delimiter=",")
            keys_not_in_columns = set(config["Csv Fields"].values()).difference(leeds_df.columns)

            # Check if necessary columns in the leeds csv
            if len(keys_not_in_columns) == 0:
                populate_unique_domains(leeds_df)
                # Add "columns" for campaign tracking
                date_n_days_ago = (datetime.now() - timedelta(days=days_between_contact)).strftime("%Y-%m-%d %H:%M:%S")
                leeds_df[engagement_date_key] = date_n_days_ago
                leeds_df[copy_stage_key] = 0

                for _, row in leeds_df.iterrows():
                    # Check if a document with the same 'name' and 'company' already exists
                    existing_document = leeds_collection.find_one({client_name_key: row[client_name_key],
                                                                   client_email_key: row[client_email_key]})

                    if existing_document:
                        logger.warning(f"Duplicate found for {row[client_name_key]} at {row[client_email_key]}. "
                                       f"Skipping insert.")
                    else:
                        # Insert if no duplicate is found
                        data_dict = row.to_dict()
                        #leeds_collection.insert_one(data_dict)
                        logger.info(f"Inserted {row[client_name_key]} at {row[client_email_key]} into "
                                    f"{leeds_collection_name}.")

                # Move file to the "processed" directory
                shutil.move(file_path, os.path.join(processed_dir, filename))
                logger.info(f"Moved {filename} to {processed_dir}.")
            else:
                logger.warning(f"Column mismatch in {filename}. Missing Columns: {keys_not_in_columns}")

        except Exception as e:
            logger.error(f"Error processing {filename}: {e.with_traceback()}")

client.close()
