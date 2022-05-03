import pymongo

url="mongodb://ia.dsa.21.a:tuJ6ZdJGWrEf8SAd6gb8ZaHUcs83HHJu@18.189.210.178:27017/?authSource=IA&readPreference=primary&appname=MongoDB%20Compass%20Community&directConnection=true&ssl=false"
client = pymongo.MongoClient(url)
db = client["UKdata"]
dbCrime = db["Crime"]

def countMonthCrime(postcode): #linechart

    list_countCrime = dbCrime.aggregate([
        {"$match":{"$and":[{"postcode":postcode},{"date":{"$lte":"2019-06"}}]}},
        {"$project":{"all_crime&asb":1,"date":"$date","_id":0}},
        {"$sort":{"_id":-1}}
    ])

    return list(list_countCrime)