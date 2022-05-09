from recommender_engine import RecommenderEngine
import json
import numpy as np

input_data = np.array([0.3 , 0.2, 0.1      , 0.5, 0,
       0, 0]) 


top_5_area = RecommenderEngine()
print(top_5_area.get_recommendations(input_data))
