import os
import sys
import json
import certifi
import pandas as pd
import pymongo
from dotenv import load_dotenv
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# Load environment variables
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")
ca = certifi.where()

class NetworkDataExtract:
    def __init__(self):
        pass

    def csv_to_json(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_to_mongodb(self, records, database, collection):
        try:
            mongo_client = pymongo.MongoClient(MONGODB_URI)
            db = mongo_client[database]
            coll = db[collection]
            coll.insert_many(records)
            return len(records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

if __name__ == "__main__":
    FILE_PATH = r"""Network_Data\phisingData.csv"""
    DATABASE = "NetworkSecurity"
    COLLECTION = "NetworkData"
    networkobj = NetworkDataExtract()

    try:
        # Convert CSV to JSON
        records = networkobj.csv_to_json(file_path=FILE_PATH)

        # Insert into MongoDB
        no_records = networkobj.insert_to_mongodb(records, DATABASE, COLLECTION)
        print(f"{no_records} records inserted successfully.")
    except Exception as e:
        raise NetworkSecurityException(e,sys)
