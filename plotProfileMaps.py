# Profile data on maps

import getProfileData as gPFD

import requests
import json

import folium


def getDataSet():
    profileDataset = gPFD.profileCleanData()
    print(profileDataset.info())

    return profileDataset


def get_coordinates():

    # setting up return value -
    locCordList = 'error'

    # getting dataset for processing location coordinates -
    getProfileSet = getDataSet()
    countPerLoc = getProfileSet['Current_Location'].value_counts().to_dict()

    getCordData = {}
    for keys in countPerLoc:
        # Make a request to the geocoding API
        setKeys = keys+",India"
        api_key = '8f93dd7094b440a99d8d146c7277c592'  # Replace with your API key
        api_url = f'https://api.opencagedata.com/geocode/v1/json?key={api_key}&q={setKeys}'
        #print(keys, ":", api_url)
        response = requests.get(api_url)
        data = json.loads(response.text)

        # Extract the coordinates from the API response
        if data['results']:
            latitude = data['results'][0]['geometry']['lat']
            longitude = data['results'][0]['geometry']['lng']
            getCordData.update({keys: [latitude, longitude]})
            locCordList = getCordData

    return locCordList


def getMapRender():
    # Create a folium map object
    map_html = 'error'
    getLocData = get_coordinates()

    dataCountCord = getDataSet()
    profileCount = dataCountCord['Current_Location'].value_counts().to_dict()

    widthSize = 250

    if len(getLocData) == 1:
        setFirstKey = getLocData.keys()
        popText = profileCount[setFirstKey]

        mapData = folium.Map(getLocData[setFirstKey], zoom_start=5)
        folium.Marker(getLocData[setFirstKey], popup=popText, max_width=widthSize).add_to(mapData)
        map_html = mapData.get_root().render()

    else:
        getLocDataList = list(getLocData.items())
        indexValSet = 0
        ikey, valueSet = list(getLocDataList[indexValSet])

        pop_text = ikey+":"+str(profileCount[ikey])

        mapData = folium.Map(valueSet, zoom_start=5)
        folium.Marker(valueSet, popup=pop_text, max_width=widthSize).add_to(mapData)

        counter = 0
        for keys in getLocData:
            if counter == 0:
                counter = counter + 1

            else:
                # Add data points to the map
                pop_text = keys+":"+str(profileCount[keys])

                folium.Marker(getLocData[keys], popup=pop_text, max_width=widthSize).add_to(mapData)
                counter = counter + 1

        # Render the map using folium's HTML iframe
        map_html = mapData.get_root().render()

    return map_html
