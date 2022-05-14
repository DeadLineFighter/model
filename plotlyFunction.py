from re import M
import sys
import os
from turtle import title

sys.path.append(sys.path.append(os.path.dirname(os.path.abspath(__file__))))

from db import *
import pymongo
import plotly.express as px
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html

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

#------Crime----------------
#1
def criMon_line(df):
    fig = px.line(df, x="date", y="all_crime&asb",
    labels={
        "all_crime&asb": "Number of crime",
        "date": "Date"
        },
    title ="Number of Crime"
    )
    return fig
#2
def criTyp_line(df):
    df_long=pd.melt(df, id_vars=['date'], value_vars=df.columns[0:-1])
    fig = px.line(df_long, x='date', y='value', color='variable',
    labels={
        "date": "Date",
        "value": "Sum of Crime Type"
        },
    title = "Number of Crime Type",
    height=600)
    return fig



#-----POI----------------
#1
def POI_type(df):
    df.sort_values(by=['count'],ascending=False,inplace=True)
    fig = px.bar(df, x="_id", y="count", color="_id",
    labels={
        "count": "Num of Industry",
        "_id": "Type of Industries"
        },
    title = "Number of Industries Type")
    return fig

#2
def poiRat_bar(df):
    df.sort_values(by=['avg'],ascending=False,inplace=True)
    df = df.round(decimals = 1)
    fig = px.bar(df, x='_id', y='avg', color='avg',
    labels={
        "_id": "Enterprise",
        "avg": "Average Score"
        },
    title = "Average Score of Enterprise")
    return fig

#3
def higRat_sca(df):
    fig = px.bar(df, x="_id", y="count", color="_id",
                labels={
                    "count": "Num of Industry",
                    "_id": "Industries Type"
                    },title = "Number of Industries Type"
                )
 
    return fig


#------School----------------
#1
def schGen_pie(df):
    fig = px.pie(df, values='count', names='_id',
        title='Number of Gender in ',
        hover_data=['_id'], labels={'_id':'Gender'})
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

#2
def schPha_bar(df):
    df.sort_values(by=['count'],ascending=False,inplace=True)
    fig = px.bar(df, x= "count", y="_id", color="_id", 
    labels={
        "count": "Sum",
        "_id": "Rating"
        },
    title = "Number of School Phase",
    orientation='h')
    return fig

#3
def schRat_bar(df):
    df.fillna('Unclassified', inplace=True)
    df.sort_values(by=['count'],ascending=False,inplace=True)
    fig = px.bar(df, x="count", y="_id", color='_id',
        labels={
        "count": "Sum",
        "_id": "Number of School Rating"
        }, title = "School Rating")
    return fig
    
    
#------Property----------------
#1
def pro_line(df):
    df.sort_values(by=['count'],ascending=False,inplace=True)
    fig = px.bar(df, x="_id", y="count", color='_id',
        labels={
            "count": "Num of Property",
            "_id": "Property Type"
        },
    title = "Number of Property Type" )
    return fig
#2
def chaPie(df):
    fig = px.pie(df, values='count', names='_id',
        title='Number of Channel ',
        hover_data=['_id'], labels={'_id':'Channel'})
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

#3
def avgPri_pieBar(df):
    if df[df.columns[0]].count() > 5:
        df.sort_values(by=['count'],ascending=False,inplace=True)
        fig = px.scatter(df, x="_id", y="count", color="_id", symbol="_id",
        labels={
            "count": "Number of Average Price Property",
            "_id": "Type of Property"
            },
        title = "Number of Average Price Property" )
        fig.update_traces(marker_size=20)
        return fig
    else:
        fig = px.pie(df, values='count', names='_id',
            title='Number of Average Price Property',
            hover_data=['_id'], labels={'_id':'Type'})
        fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

