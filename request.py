from recommender_engine import RecommenderEngine
import json
import numpy as np

input_data = np.array([22, 1604, 3.25, 5.0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]) 
def get_recommendations(input_data):
    result = RecommenderEngine.get_recommendations(input_data)
    return result



top_5_area = get_recommendations(input_data)
print(top_5_area)
