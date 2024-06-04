from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError
import pandas as pd
import json
from typing import Any, List, Dict


class MongoOperation:
    __database = None
    __collection = None

    def __init__(self, client_url: str, database_name: str):
        self.client_url = client_url
        self.database_name = database_name

    def create_mongo_client(self):
        return MongoClient(self.client_url)

    def create_database(self):
        if MongoOperation.__database is None:
            client = self.create_mongo_client()
            MongoOperation.__database = client[self.database_name]
        return MongoOperation.__database

    def collection_exists(self, collection_name: str) -> bool:
        database = self.create_database()
        collection_names = database.list_collection_names()
        return collection_name in collection_names

    def create_collection(self, collection_name: str):
        if not self.collection_exists(collection_name):
            print(f"Do you want to create a new Collection '{collection_name}'? (Y/N)")
            user_input = input().strip().upper()
            if user_input == 'Y':
                print(f"Your new collection '{collection_name}' has been created!")
            elif user_input == 'N':
                return None
            else:
                print("Invalid input. Returning without creating a collection.")
                return None

        MongoOperation.__collection = self.create_database()[collection_name]
        return MongoOperation.__collection

    def insert_record(self, record: Any, collection_name: str):
        collection = self.create_collection(collection_name)
        if collection is not None:
            if isinstance(record, list):
                if not all(isinstance(data, dict) for data in record):
                    raise TypeError("All records must be dictionaries")
                try:
                    collection.insert_many(record)
                except PyMongoError:
                    print("There is duplicate data")
            elif isinstance(record, dict):
                try:
                    collection.insert_one(record)
                except PyMongoError:
                    print("This is a duplicate data in your collection")
            else:
                raise TypeError("Record must be a dictionary or a list of dictionaries")

    def bulk_insert(self, datafile: str, collection_name: str):
        if datafile.endswith('.csv'):
            dataframe = pd.read_csv(datafile, encoding='utf-8')
        elif datafile.endswith(".xlsx"):
            dataframe = pd.read_excel(datafile, encoding='utf-8')
        else:
            raise ValueError("Unsupported file format")

        datajson = json.loads(dataframe.to_json(orient='records'))
        collection = self.create_collection(collection_name)
        if collection:
            collection.insert_many(datajson)

    def read_data(self, collection_name: str, filter: dict = None, projection: dict = None,
                  sort: List[tuple] = None, limit: int = 0, skip: int = 0,
                  count: bool = False, distinct: str = None) -> List[Dict]:
        collection = self.create_collection(collection_name)
        if collection is not None:
            query = collection.find(filter, projection).skip(skip)

            if sort:
                for field, order in sort:
                    if order == 'asc':
                        query = query.sort(field, ASCENDING)
                    elif order == 'desc':
                        query = query.sort(field, DESCENDING)

            if limit:
                query = query.limit(limit)

            if count:
                count = query.count_documents()
                print(f"Total documents: {count}")

            if distinct:
                distinct_values = collection.distinct(distinct, filter)
                print(f"Distinct values of {distinct}: {distinct_values}")

            results = list(query)
            for doc in results:
                print(doc)

            return results
        return []

    def update_data(self, collection_name: str, filter: dict, update: dict, upsert: bool = False) -> int:
        try:
            collection = self.create_collection(collection_name)
            if collection is None:
                raise ValueError("Collection not found.")

            result = collection.update_many(filter, {"$set": update}, upsert=upsert)
            if result:
                return result.modified_count
            return 0
        except PyMongoError as e:
            print(f"An error occurred: {e}")
            return -1
        except ValueError as ve:
            print(f"Value error: {ve}")
            return -1

    def delete_data(self, collection_name: str, filter: dict) -> int:
        try:
            collection = self.create_collection(collection_name)
            if collection is None:
                raise ValueError("Collection not found.")

            result = collection.delete_many(filter)
            if result:
                return result.deleted_count
            return 0
        except PyMongoError as e:
            print(f"An error occurred: {e}")
            return -1
        except ValueError as ve:
            print(f"Value error: {ve}")
            return -1
