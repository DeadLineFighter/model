import pymongo

url="mongodb://ia.dsa.21.a:tuJ6ZdJGWrEf8SAd6gb8ZaHUcs83HHJu@18.189.210.178:27017/?authSource=IA&readPreference=primary&appname=MongoDB%20Compass%20Community&directConnection=true&ssl=false"
client = pymongo.MongoClient(url)
db = client["UKdata"]
dbCrime = db["Crime"]

listCrime = dbCrime.distinct('Crime type')

countCrime = dbCrime.count_documents({'Crime type':listCrime[0]})

numCrime = []

for i in range(len(listCrime)):
    numCrime.append(dbCrime.count_documents({'Crime type':listCrime[i]}))

crimeDate = zip(listCrime, numCrime)
dictCrimeDate = dict(crimeDate)

dictCrimeDate