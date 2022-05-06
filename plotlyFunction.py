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
    return fig.show()

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

#test msg