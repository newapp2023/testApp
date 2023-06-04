import pymongo.errors
from pymongo import MongoClient

# Database details -
mongoDbName = 'profileset'
global_mongo_uri = "mongodb+srv://dbuser1:NewApp4545@dataapp1.uviczfh.mongodb.net/?retryWrites=true&w=majority"

def getDatabaseFn():

    mongoDBReturn = 'error'

    try:
        client = MongoClient(global_mongo_uri, serverSelectionTimeoutMS=2000)
        client.server_info()
        mongoDBReturn = client[mongoDbName]

    except pymongo.errors.ServerSelectionTimeoutError as Con_err:
        print("DB Connect Problem")
        print(Con_err)

    return mongoDBReturn
