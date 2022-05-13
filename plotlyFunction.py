from re import M
import sys
import os

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

#------Crime----------------
#1
def crime_month_line(df):
    fig = px.line(df, x="date", y="all_crime&asb",
    labels={
        "all_crime&asb": "Number of crime",
        "date": "Date"
        },
    title =""
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
    title = "title",
    height=600)
    return fig



#-----POI----------------
#1
def POI_type(df):
    df.sort_values(by=['count'],ascending=False,inplace=True)
    fig = px.bar(df, x="_id", y="count", color="_id",
    labels={
        "count": "Num of industry",
        "_id": "Type of industries"
        },
    title = "Number of type of industries in ")
    return fig

#2
def poiRat_bar(df):
    df.sort_values(by=['avg'],ascending=False,inplace=True)
    df = df.round(decimals = 1)
    fig = px.bar(df, x='_id', y='avg', color='avg',
    labels={
        "_id": "Enterprise",
        "avg": "Average score"
        },
    title = "Average score of Enterprise in ")
    return fig

#3
def higRat_sca(df):
    fig = px.bar(df, x="_id", y="count", color="_id",
                labels={
                    "count": "Num of industry",
                    "_id": "Type of industries"
                    },title = "Number of type of industries in "
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
    title = "School Phase in ",
    orientation='h')
    return fig

#3
def schRat_bar(df):
    df.fillna('Unclassified', inplace=True)
    df.sort_values(by=['count'],ascending=False,inplace=True)
    fig = px.bar(df, x="count", y="_id", color='_id', orientation='h',
        labels={
        "count": "Sum",
        "_id": "School Rating"
        }, title = "School Rating in ")
    return fig
    
    
#------Property----------------
#1
def pro_line(df):
    df.sort_values(by=['count'],ascending=False,inplace=True)
    fig = px.bar(df, x="_id", y="count",
        labels={
            "count": "Num of Property",
            "_id": "Property Type"
        },
    title = "Number of Property Type in " )
    return fig
#2
def chaPie(df):
    fig = px.pie(df, values='count', names='_id',
        title='Number of Channel in ',
        hover_data=['_id'], labels={'_id':'Channel'})
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

#3
def avgPri_pieBar(df):
    if df[df.columns[0]].count() > 5:
        df.sort_values(by=['count'],ascending=False,inplace=True)
        fig = px.scatter(df, x="_id", y="count", color="_id", symbol="_id",
        labels={
            "count": "Number of average price property",
            "_id": "Type of property"
            },
        title = "Number of average price property in " )
        fig.update_traces(marker_size=20)
        return fig
    else:
        fig = px.pie(df, values='count', names='_id',
            title='Number of average price property in ',
            hover_data=['_id'], labels={'_id':'Type'})
        fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig