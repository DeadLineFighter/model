from recommender_engine import RecommenderEngine
import json
import numpy as np

input_data = np.array([0.        , 0.16666667, 0.18      , 0.44567219, 0.19751166,
       0.40701754, 0.66938776]) 


top_5_area = RecommenderEngine()
print(top_5_area.get_recommendations(input_data))
