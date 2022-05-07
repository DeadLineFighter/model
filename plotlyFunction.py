from re import M
import sys
import os
sys.path.append(sys.path.append(os.path.dirname(os.path.abspath(__file__))))

from db import *

def crime_month_line(postcode):
    df = pd.DataFrame(countMonthCrime(postcode))
    fig = px.line(df, x="date", y="all_crime&asb",
    labels={
        "all_crime&asb": "Number of crime",
        "date": "Date"
        },
    title = "Number of crime in every month in "+postcode)
    return fig

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