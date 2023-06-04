#  Checking Profile Data

import pymongo.errors
from pymongo import MongoClient
import json
import pandas as pd


def checkJobData():
    mongoDBReturn = 'error'
    mongoDbName = 'profileset'
    global_mongo_uri = "mongodb+srv://dbuser1:NewPass4545@dataapp1.uviczfh.mongodb.net/?retryWrites=true&w=majority"
    client = 'error'
    db_status_inner = True
    json_results = ''
    results_array = 'error'

    try:
        client = MongoClient(global_mongo_uri, serverSelectionTimeoutMS=2000)
        mongoDBReturn = client[mongoDbName]

    except pymongo.errors.ServerSelectionTimeoutError as Con_err:
        print("Error: DB Connection Time-Out", Con_err)
        db_status_inner = False
        exit()

    except pymongo.errors.ConnectionFailure as DB_Con_err:
        print("Error: DB Connection Failed", DB_Con_err)
        db_status_inner = False
        exit()

    if db_status_inner:
        collectionName = 'jobdata'
        collection = mongoDBReturn.get_collection(collectionName)
        dbResultSet = collection.find()

        results_array = list(dbResultSet)

        json_results = []
        for result in dbResultSet:
            json_results.append(json.dumps(result, default=str))

    return results_array


def checkJobCsvFile():
    csv_file_path = "F:\\hrInfoApp\\04_data\\job_req_notes\\job_req_data_check.csv"
    df_jobDataSet = pd.read_csv(csv_file_path)

    return df_jobDataSet


def getJobDataSet():

    # setting up return dataset for function -
    dfJobDataSet = 'error'
    getDataByJob = checkJobData()

    if not getDataByJob == 'error':

        dfJobDataSet = pd.DataFrame(getDataByJob)

        if len(dfJobDataSet) > 0:
            print(dfJobDataSet.info())
            print(dfJobDataSet["client"])

            counter = 0
            fileNameList = []
            for val in dfJobDataSet["client"]:
                if val == "LTI":
                    fileName_prefix = "lnt_jr_"
                    fileName = fileName_prefix+str(dfJobDataSet.iloc[counter, dfJobDataSet.columns.get_loc("jr_number")])
                    fileNameList.append(fileName)
                counter = counter + 1

            dfJobDataSet['file_name'] = fileNameList

    return dfJobDataSet


def jobListData():

    # setting the return value of function -
    listSetForPage = 'error'

    getJobListData = getJobDataSet()

    if not len(getJobListData) == 0:
        listSetForPage = getJobListData[["entry_date", "status", "client", "rmg_name", "job_title", "jr_number", "primary_skill", "notice_period_inDays"]]

    indexList = listSetForPage.columns.to_list()
    print(indexList)

    makeDataDict = {}
    makeDataList = []
    setCount = 0
    while setCount < len(listSetForPage):
        for val in indexList:
            getValText = listSetForPage.loc[setCount, val]
            makeDataList.append(getValText)
        makeDataDict.update({str(setCount): makeDataList})
        makeDataList = []
        setCount = setCount + 1
    setReturnPack = {}

    setReturnPack.update({"job-data": makeDataDict})
    setReturnPack.update({"header-text": indexList})

    return setReturnPack