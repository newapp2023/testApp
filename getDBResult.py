import pyMongoConnect as pMC

def getConnectFn():
    # Function for DB connection and Mongo Client -
    getDBSet = pMC.getDatabaseFn()
    return getDBSet


def getResult():
    # Setting up collection and getting result-set
    dbSetVal = getConnectFn()

    # getting the latest collection name by using the index ID
    collectionFile = "profiledata"
    dbCollectionName = dbSetVal[collectionFile]

    # getting result from the collection -
    dbResultSet = dbCollectionName.find()

    ListItems = []
    strListItems = ''
    for items in dbResultSet:
        ListItems.append(items)
        strListItems = strListItems+",{"+str(items)+"}"

    return ListItems


if __name__ == '__main__':
    textResult = getResult()
    print(len(textResult))
