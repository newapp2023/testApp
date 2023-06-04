#  Checking Profile Data
import numpy as np
import pymongo.errors
from pymongo import MongoClient
import json
import pandas as pd


def checkProfileData():

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
    collectionName = 'profiledata'
    collection = mongoDBReturn.get_collection(collectionName)
    dbResultSet = collection.find()

    results_array = list(dbResultSet)

    json_results = []
    for result in dbResultSet:
      json_results.append(json.dumps(result, default=str))
  else:
    dbResultSet = client

  return results_array


def profileStat():
    # setting the output return value for this function -
    return_PackDict = 'error'

    profileDataSet = checkProfileData()
    profileSet_df = pd.DataFrame(profileDataSet)

    profileSet_df.columns = profileSet_df.columns.str.strip()
    profileSet_df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

    profileSet_df["Skill_Set"].replace('SAP_ABAP ', 'SAP_ABAP', inplace=True)
    profileSet_df["Skill_Set"].replace('SAP_Abap', 'SAP_ABAP', inplace=True)
    profileSet_df["Skill_Set"].replace('SAP_Fico', 'SAP_FICO', inplace=True)
    profileSet_df["Skill_Set"].replace('ERP Consultant', 'ERP', inplace=True)
    profileSet_df["Notice_Period"].replace('Immediate', 0, inplace=True)

    profileSet_df["Current_Location"].replace('New Delhi ', 'Delhi', inplace=True)
    profileSet_df["Current_Location"].replace('Delhi/NCR', 'Delhi', inplace=True)
    profileSet_df["Current_Location"].replace('New Delhi', 'Delhi', inplace=True)
    profileSet_df["Current_Location"].replace('Hyderabad/Secunderabad', 'Hyderabad', inplace=True)
    profileSet_df["Current_Location"].replace('Hyd', 'Hyderabad', inplace=True)
    profileSet_df["Current_Location"].replace('Bangalore/Bengaluru', 'Bangalore', inplace=True)
    profileSet_df["Current_Location"].replace('Gurgaon/Gurugram', 'Gurugram', inplace=True)
    profileSet_df["Current_Location"].replace('Bellary/Ballari', 'Ballari', inplace=True)
    profileSet_df["Current_Location"].replace('New Delhi', 'Delhi', inplace=True)

    recordsSkillSet_df = profileSet_df['Skill_Set']

    #   This is to get the Group by record count:
    outputData = recordsSkillSet_df.value_counts().to_dict()

    if not len(profileSet_df) == 0:

        # getting data by location and skill set -
        dfLocationDataSet = profileSet_df.copy()
        dataIndicator = "Current_Location"
        selectIndicator = "Skill_Set"

        locationList = sorted(dfLocationDataSet[dataIndicator].unique().tolist())
        skillList = dfLocationDataSet[selectIndicator].unique().tolist()
        skillListData = ''

        for strVal in skillList:
            skillListData = skillListData+","+strVal

        groupBySkillSet = dfLocationDataSet.groupby(selectIndicator)

        skillCountByLoc = {}
        skillLocData = ''
        skillLocTotalData = ""

        skillCountForLoc = []
        locSkillDict = {}

        for skillVal in skillList:
            skillDataPerVal = groupBySkillSet.get_group(skillVal)
            df_SkillSetTemp = pd.DataFrame(skillDataPerVal)
            for locVal in locationList:
                getRecordCount = df_SkillSetTemp.query(f'{dataIndicator} == "{locVal}"')

                skillCountPerLoc = len(getRecordCount)
                skillCountForLoc.append(skillCountPerLoc)
                skillLocData = skillLocData + "," + str(skillCountPerLoc)

            locSkillDict.update({skillVal: skillCountForLoc})
            skillCountForLoc = []

            skillLocTotalData = skillLocTotalData + "|" + skillLocData[1:]
            skillLocData = ''

        # getting skill set for notice period range-
        getRawDataSet = profileSet_df

        getNoticePeriodList = sorted(getRawDataSet["Notice_Period"].unique().tolist())

        groupBySkillSet = getRawDataSet.groupby("Skill_Set")

        countList = []
        dictSetSkillCount = {}
        strCountSet = ''
        df_column_head = [str(item) for item in getNoticePeriodList]

        keys_temp = outputData

        # Declaring array set for use in the loop to collect values and v-stack operations
        arraySet = np.arange(0, len(getNoticePeriodList))

        iCount = 0
        for keys in keys_temp:
            tempSet = groupBySkillSet.get_group(keys)
            df_temp = pd.DataFrame(tempSet)
            for Val in getNoticePeriodList:
                getValCount = df_temp.query(f'Notice_Period == {Val}')
                countList.append(len(getValCount))
                strCountSet = strCountSet + "," + str(len(getValCount))

            dictSetSkillCount.update({keys: countList})
            if iCount == 0:
                arraySet = np.array(countList)
            else:
                array_temp = np.array(countList)
                arraySet = np.vstack((arraySet, array_temp))
            countList = []
            strCountSet = ''
            iCount = iCount + 1

        # Data DF for all modules as per keys -
        dfArraySet = pd.DataFrame(arraySet, columns=[df_column_head], index=keys_temp.keys())

        return_PackDict = {}
        listDataPack = {}
        statPack = {}

        getSapList = ''
        getSapValList = ''
        for keyVal in keys_temp:
            getSapList = getSapList + "," + keyVal
            getValList = dfArraySet.loc[keyVal]
            temp_List = list(getValList)
            listDataPack.update({keyVal: temp_List})
            statPack.update({keyVal: sum(temp_List)})
            getSapValList = getSapValList + "," + str(sum(temp_List))

        setDataForGraph = {}
        setGraphList = []
        for keys in statPack:
            setDataForGraph.update({'y': statPack[keys], 'label': keys})
            setGraphList.append(setDataForGraph)
            setDataForGraph = {}

        # print(setGraphList)

        return_PackDict.update({"stat": statPack})
        return_PackDict.update({"data": listDataPack})
        return_PackDict.update({"graphLabel": getSapList[1:]})
        return_PackDict.update({"graphVal": getSapValList[1:]})

        df_column_head_new = ['SAP']
        df_column_head_new.extend(df_column_head)

        return_PackDict.update({"noticeRange": df_column_head_new})
        #return_PackDict.update({"fullSet": profileSet_df})

        locationList_new = ['SAP']
        locationList_new.extend(locationList)

        return_PackDict.update({"loc-list": locationList_new})
        return_PackDict.update({"loc-stat": skillLocTotalData[1:]})
        return_PackDict.update({"skill-List": skillListData[1:]})
        return_PackDict.update({"skill-List2": skillList})
        return_PackDict.update({"col-span": len(skillList)})
        return_PackDict.update({"skill-loc-count": locSkillDict})

    return return_PackDict