import pandas as pd
import pymongo
from cosine_similarity import CosineSimilarity
import operator
import json

class RecommenderEngine:
    def __init__(self):
        print("engine initialized")

    def get_recommendations(input_data):
        url="mongodb://ia.dsa.21.a:tuJ6ZdJGWrEf8SAd6gb8ZaHUcs83HHJu@18.189.210.178:27017/?authSource=IA&readPreference=primary&appname=MongoDB%20Compass%20Community&directConnection=true&ssl=false"
        client = pymongo.MongoClient(url)
        mydb = client["IA"]
        mycol = mydb["postcode_UK_Data"]
        df = pd.json_normalize(mycol.find())
        
        real_df=df.drop(['_id','name','property_type', 'channel','Crime_rate'], axis=1)
        train = pd.get_dummies(df[["property_type","channel"]])
        pd.DataFrame(train)
        final_df=real_df.merge(train, left_index=True, right_index=True)
   
        score_dict = {}
        for index, row in df.iterrows():
            score_dict[index] = CosineSimilarity.cos_sim(final_df.iloc[index].astype(float), input_data)


        #sort cities by score and index.
        sorted_scores = sorted(score_dict.items(), key=operator.itemgetter(1), reverse=True)

        counter = 0

        #create an empty results data frame.
        resultDF = pd.DataFrame(columns=("postcode","score"))#result Data colummns

        #get highest scored 5 cities.
        for i in sorted_scores:
            #print index and score of the city.
            #print(i[0], i[1])
            resultDF = resultDF.append({'postcode': df.iloc[i[0]]['name'], 'score': i[1]}, ignore_index=True)
            counter += 1

            if counter>4:
                break

        #convert DF to json.
        json_result = json.dumps(resultDF.to_dict('records'))
        return json_result
