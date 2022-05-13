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

    return list(monthCrime) #e.g countMonthCrime("LS1")

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

    return count #e.g countAllCrime("LS1")

def countCrimeType(postcode):

    crimeType = dbCrime.aggregate([
        {"$match":{"$and":[{"postcode":postcode},{"date":{"$lte":"2019-06"}}]}},
        {"$project":{"_id":0,
        "anti_social_behaviour":"$anti_social_behaviour",
        "burglary":"$burglary",
        "robbery":"$robbery",
        "vehicle_crime":"$vehicle_crime",
        "violent_crime":"$violent_crime",
        "shoplifting":"$shoplifting",
        "criminal_damage&arson":"$criminal_damage&arson",
        "other_theft":"$other_theft",
        "drug_crimes":"$drug_crimes",
        "bike_theft":"$bike_theft",
        "theft_from_the_person":"$theft_from_the_person",
        "possession_of_weapons":"$possession_of_weapons",
        "public_order":"$public_order",
        "other":"$other",
        "date":"$date"}}
    ])

    return list(crimeType) #e.g countCrimeType("LS1")

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

def rightmoveChannel(postcode): 

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry)
    
    psToChannel = propertyCol.aggregate([
    {"$match":{"geometry":{
                "$geoWithin":{
                "$geometry":list_dbGeometry[0]["geometry"]}}}
                },
                {"$project":{"_id": 0, 
                            "channel":1}},
    {"$group":{"_id":"$channel","count":{"$sum":1}}}
    ])

    return list(psToChannel) #e.g rightmoveChannel("LS1")

def rightmoveAvgPrice(postcode): #data from https://www.gov.uk/government/news/uk-house-price-index-for-february-2022
                                #(there has been an annual price rise of 10.9% which makes the average property in the UK valued at £276,755)

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry)
    
    psToAvgPrice = propertyCol.aggregate([
    {"$match":{"$and":[{"geometry":{
                "$geoWithin":{
                "$geometry":list_dbGeometry[0]["geometry"]}}},
                {"price":{"$gte":250000}},
                {"price":{"$lte":300000}}
                ]}},
                {"$project":{"_id": 0, 
                            "combine_property_type":1}},
    {"$group":{"_id":"$combine_property_type","count":{"$sum":1}}}
    ])

    return list(psToAvgPrice)

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

    return list(rating) #e.g schoolRating("LS1")

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

def poiRatingAvg(postcode):
    
    avg = dbGooPOI.aggregate([
        {"$match":{"postcode":postcode}},
        {"$project":{"_id":0,"label_type":1,"rate":"$rating","type":"$label_types"}}, 
        {"$group":{"_id":"$type","avg":{"$avg":"$rate"}}}
    ])

    return list(avg) #e.g poiRatingAvg("LS1")

def highScoreRatingType(postcode): #rating greater than 4.0 counting

    highScore = dbGooPOI.aggregate([
        {"$match":{"$and":[{"postcode":postcode},{"rating":{"$gte":4.0}}]}},
        {"$project":{"_id":0,"label_type":1,"rate":"$rating","type":"$label_types"}}, 
        {"$group":{"_id":"$type","count":{"$sum":1}}},
        {"$sort":{"count":-1}}
    ])

    return list(highScore) #e.g highScoreRatingType("LS1")

def rightmoveChannel(postcode): 

    dbGeometry = geoCol.find({"name":postcode})
    list_dbGeometry = list(dbGeometry)

    psToChannel = propertyCol.aggregate([
    {"$match":{"geometry":{
                "$geoWithin":{
                "$geometry":list_dbGeometry[0]["geometry"]}}}
                },
                {"$project":{"_id": 0, 
                            "channel":1}},
    {"$group":{"_id":"$channel","count":{"$sum":1}}}
    ])

    return list(psToChannel) #e.g rightmoveChannel("LS1")