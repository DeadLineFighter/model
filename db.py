import pymongo

url="mongodb://ia.dsa.21.a:tuJ6ZdJGWrEf8SAd6gb8ZaHUcs83HHJu@18.189.210.178:27017/?authSource=IA&readPreference=primary&appname=MongoDB%20Compass%20Community&directConnection=true&ssl=false"
client = pymongo.MongoClient(url)
db = client["UKdata"]
dbCrime = db["Crime"]

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