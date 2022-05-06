import pymongo
import plotly.express as px
import pandas as pd

url="mongodb://ia.dsa.21.a:tuJ6ZdJGWrEf8SAd6gb8ZaHUcs83HHJu@18.189.210.178:27017/?authSource=IA&readPreference=primary&appname=MongoDB%20Compass%20Community&directConnection=true&ssl=false"
client = pymongo.MongoClient(url)
db = client["UKdata"]
dbCrime = db["uk_crime"]
dbGooPOI = db["GooglePOI"]
geoCol = db["UkGEO"]
school = db["UK_Schools"]
#----------------------
testDb = client["IA"]
propertyCol = testDb["Rightmove_15cities Backup"]


def rightmoveLatLongAndGeo(postcode): #for map plotly

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry)

    psToRightmove = propertyCol.aggregate([
    {"$match":{"geometry":{
                "$geoWithin":{
                "$geometry":list_dbGeometry[0]["geometry"]}}}},
                {"$project":{"_id": 0, 
                            "latitude": 1,
                            "longitude": 1}}])
                            
    return list_dbGeometry[0]["geometry"]["coordinates"][0] + list(psToRightmove) #e.g rightmoveLatLongAndGeo("LS1")
    #First one is GEOmetry, second is job place

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

def countAllCrime(postcode): #count all crime date with postcode

    count = 0

    allCrime = dbCrime.aggregate([
        {"$match":{"$and":[{"postcode":postcode},{"date":{"$lte":"2019-06"}}]}},
        {"$project":{"all_crime&asb":1,"date":"$date","_id":0}},
        {"$sort":{"_id":-1}}        
    ])

    listAllCrime = list(allCrime)

    for i in range(len(listAllCrime)):

        count += listAllCrime[i]['all_crime&asb']

    return count #e.g countAllCrime("BL0")

def countPoiType(postcode):

    poiType = dbGooPOI.aggregate([
        {"$match":{"postcode":postcode}},
        {"$project":{"_id":0,"label_types":1,"type":"$label_types"}}, 
        {"$group":{"_id":"$label_types","count":{"$sum":1}}}
    ])

    return list(poiType) #e.g countPoiType("WN2")

def rightmoveProperty(postcode): #count property type

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry)
    
    psToRightmove = propertyCol.aggregate([
    {"$match":{"geometry":{
                "$geoWithin":{
                "$geometry":list_dbGeometry[0]["geometry"]}}}},
                {"$project":{"_id": 0, 
                            "combine_property_type":1}},
    {"$group":{"_id":"$combine_property_type","count":{"$sum":1}}}
    ])

    return list(psToRightmove) #e.g rightmoveProperty("LS1")

def schoolPhase(postcode):

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry) 

    phase = school.aggregate([
    {"$match":{"geometry":{
                "$geoWithin":{
                "$geometry":list_dbGeometry[0]["geometry"]}}}},
                {"$project":{"_id": 0, 
                            "PhaseOfEducation (name)":1}},
    {"$group":{"_id":"$PhaseOfEducation (name)","count":{"$sum":1}}}
    ])

    return list(phase) #e.g schoolPhase("LS2")

def schoolRating(postcode):

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry) 

    rating = school.aggregate([
    {"$match":{"geometry":{
                "$geoWithin":{
                "$geometry":list_dbGeometry[0]["geometry"]}}}},
                {"$project":{"_id": 0, 
                            "OfstedRating (name)":1}},
    {"$group":{"_id":"$OfstedRating (name)","count":{"$sum":1}}}
    ])

    return list(rating) #e.g schoolRating("BL0")

def schoolGender(postcode):

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry) 

    gender = school.aggregate([
    {"$match":{"geometry":{
                "$geoWithin":{
                "$geometry":list_dbGeometry[0]["geometry"]}}}},
                {"$project":{"_id": 0, 
                            "Gender (name)":1}},
    {"$group":{"_id":"$Gender (name)","count":{"$sum":1}}}
    ])

    return list(gender) #e.g schoolGender("LS3")