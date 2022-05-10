from re import M
import sys
import os
from db import *
import pymongo
import plotly.express as px
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

sys.path.append(sys.path.append(os.path.dirname(os.path.abspath(__file__))))

url="mongodb://ia.dsa.21.a:tuJ6ZdJGWrEf8SAd6gb8ZaHUcs83HHJu@18.189.210.178:27017/?authSource=IA&readPreference=primary&appname=MongoDB%20Compass%20Community&directConnection=true&ssl=false"
client = pymongo.MongoClient(url)
db = client["UKdata"]
dbCrime = db["uk_crime"]
dbGooPOI = db["GooglePOI"]
school = db["UK_Schools"]
geoCol = db["UkGEO"]

testDb = client["IA"]
propertyCol = testDb["Rightmove_15cities Backup"]

#---------------------

def changeLatLong(postcode):

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry)

    for i in range(len(list_dbGeometry[0]["geometry"]["coordinates"][0])):
        list_dbGeometry[0]["geometry"]["coordinates"][0][i][0], list_dbGeometry[0]["geometry"]["coordinates"][0][i][1] = list_dbGeometry[0]["geometry"]["coordinates"][0][i][1], list_dbGeometry[0]["geometry"]["coordinates"][0][i][0]

    result = list_dbGeometry[0]["geometry"] #change place

    return result
#----------------------

def countMonthCrime(postcode): #suggest use linechart to plotly

    monthCrime = dbCrime.aggregate([
        {"$match":{"$and":[{"postcode":postcode},{"date":{"$lte":"2019-06"}}]}}, #as after 2019-06, the date is not real
        {"$project":{"all_crime&asb":1,"date":"$date","_id":0}},
        {"$sort":{"_id":-1}}
    ])

    return list(monthCrime) #e.g countMonthCrime("BL0")


#---
def crime_month_line(postcode):
    df = pd.DataFrame(countMonthCrime(postcode))
    fig = px.line(df, x="date", y="all_crime&asb",
    labels={
        "all_crime&asb": "Number of crime",
        "date": "Date"
        },
    title = "Number of crime in every month in "+postcode)
    return fig

crime_month_line("BL0").write_html('first_figure.html', auto_open=True)





def crime_many_month_line(postcodes):
    list_of_dfs = list()
    toString = list()
    for x in postcodes:
        toString.append(x)
        df = pd.DataFrame(countMonthCrime(x))
        df["Postcode"] = x
        list_of_dfs.append(df)
        combine = pd.concat(list_of_dfs)
    str = ' '.join(toString)
    fig = px.line(combine, x="date", y="all_crime&asb", color='Postcode',
    labels={
        "all_crime&asb": "Number of crime",
        "date": "Date"
        },
    title = "Number of crime in every month in "+str)
    return fig

def POI_type(postcode):
    df = pd.DataFrame(countPoiType(postcode))
    df.sort_values(by=['count'],ascending=False,inplace=True)
    fig = px.bar(df, x="_id", y="count", color="_id",
    labels={
        "count": "Num of industry",
        "_id": "Type of industries"
        },
    title = "Number of type of industries in "+postcode)
    return fig

def property_pie_bar(postcode):
    df = pd.DataFrame(rightmoveProperty(postcode))
    if df[df.columns[0]].count() >14:
        df.sort_values(by=['count'],ascending=False,inplace=True)
        fig = px.bar(df, x="_id", y="count", color="_id",
        labels={
            "count": "Num of property",
            "_id": "Type of property"
            },
        title = "Number of type of properties in " + postcode)
        return fig
    else:
        fig = px.pie(df, values='count', names='_id',
                    title='Number of property in '+postcode,
                    hover_data=['_id'], labels={'_id':'Property type'})
        fig.update_traces(textposition='inside', textinfo='percent+label')
        return fig

def school_gender_pie(postcode):
    df = pd.DataFrame(schoolGender(postcode))
    fig = px.pie(df, values='count', names='_id',
        title='Number of gender in '+postcode,
        hover_data=['_id'], labels={'_id':'Gender'})
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

#test msg