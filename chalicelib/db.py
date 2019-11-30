from pymongo import MongoClient
import os

mongoConnection = os.environ.get('mongoConnection')
client = MongoClient(mongoConnection)
mongo = client.get_database()

