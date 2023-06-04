# from bson import Binary, Code
# from bson.json_util import dumps
from flask import Flask, request, render_template

import getProfileData as getProfileSet
import plotProfileMaps as getMapData
import getJobData as getJobSet


# setting up app and config for mongo db
app = Flask(__name__)



# Main Page to be used for online demo:

# This is Resource Center main page with Stats and graphs -
@app.route("/", methods=('GET', 'POST'))
def mainPage():
  if request.method == 'GET':
    #data = dumps(gpd.profileStat())
    data = getProfileSet.profileStat()
    #data = getDatabaseFn()
    return render_template('newhome.html', data=data)
#    return render_template('checkHome.html', data=data)

# This is for testing maps functionality -
@app.route("/mapinfo", methods=('GET', 'POST'))
def getMaps():
    map_html = getMapData.getMapRender()
    return render_template('mapsInfo.html', map_html=map_html)

# This is for Job Listing page -
@app.route("/job-page", methods=('GET', 'POST'))
def jobPage():
    if request.method == 'GET':
        data = getJobSet.jobListData()
        return render_template('jobListPage.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)
