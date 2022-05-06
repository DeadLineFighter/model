import pandas as pd
import pymongo
from cosine_similarity import CosineSimilarity
import operator
import json
from functools import reduce


class RecommenderEngine:
    def __init__(self):
        print("engine initialized")
        self.school_data= pd.read_csv("school_data.csv")

        url="mongodb://ia.dsa.21.a:tuJ6ZdJGWrEf8SAd6gb8ZaHUcs83HHJu@18.189.210.178:27017/?authSource=IA&readPreference=primary&appname=MongoDB%20Compass%20Community&directConnection=true&ssl=false"
        client = pymongo.MongoClient(url)
        mydb = client["UKdata"]
        mycol = mydb["GooglePOI"]
        self.google_poi = pd.DataFrame(list(mycol.find({},{"label_types":1,"rating":1,"postcode":1})))

        mycol = mydb["OSM_Transportation"]
        self.traffic_data = pd.DataFrame(list(mycol.find({},{"Postcode":1})))

    def return_df(self,dataframe,codintion,rename):

        finish_dataframe = dataframe[codintion].groupby('postcode',as_index=False).count()[['postcode','_id']]
        finish_dataframe.rename(columns={"_id":rename}, inplace=True)
        return finish_dataframe

    def data_process(self,data2,df,data_OSM):
        #education
        school = data2[data2['OfstedRating (name)'] >= 3].groupby('name',as_index=False).count()[['name','index']]
        school['index'] = school['index'] / school['index'].abs().max()
        school.rename(columns={"name":'postcode',"index":"school"}, inplace=True)

        #diet
        c1 = (df['label_types'].str.contains(pat = 'meal delivery') & (df['rating'] >= 3.5))
        c3 = (df['label_types'].str.contains(pat = 'bakery') & (df['rating'] >= 3.5))
        c4 = (df['label_types'].str.contains(pat = 'bar') & (df['rating'] >= 3.5))
        c5 = (df['label_types'].str.contains(pat = 'cafe') & (df['rating'] >= 3.5))
        df1 = self.return_df(df,c1,"meal delivery")
        df3 = self.return_df(df,c3,"bakery")
        df4 = self.return_df(df,c4,"bar")
        df5 = self.return_df(df,c5,"cafe")

        dfs = [df1,  df3, df4, df5]
        df_diet = reduce(lambda left,right: pd.merge(left,right,on='postcode'), dfs) 
        c2 = (df['label_types'].str.contains(pat = 'amusement park') & (df['rating'] >= 3.5))
        c20 = (df['label_types'].str.contains(pat = 'rv park') & (df['rating'] >= 3.5))
        c21 = (df['label_types'].str.contains(pat = 'park') & (df['rating'] >= 3.5))

        df16 = self.return_df(df,c2,"amusement park")
        df17 = self.return_df(df,c2,"rv park")
        df18 = self.return_df(df,c2,"park")

        dfs = [df16, df17, df18]
        df_park = reduce(lambda left,right: pd.merge(left,right,on='postcode'), dfs) 
        #shopping 
        c6 =(df['label_types'].str.contains(pat = 'convenience store') & (df['rating'] >= 3.5))
        c7 =(df['label_types'].str.contains(pat = 'department store') & (df['rating'] >= 3.5))
        c9 =(df['label_types'].str.contains(pat = 'shopping mall') & (df['rating'] >= 3.5))
        c10 =(df['label_types'].str.contains(pat = 'supermarket') & (df['rating'] >= 3.5))        
        df6 = self.return_df(df,c6,"convenience store")
        df7 = self.return_df(df,c7,"department store")
        df8 = self.return_df(df,c9,"shopping mall")
        df9 = self.return_df(df,c10,"supermarket")

        dfs = [df6, df7, df8, df9]
        df_shopping = reduce(lambda left,right: pd.merge(left,right,on='postcode'), dfs) 
        #Medical
        c11 =(df['label_types'].str.contains(pat = 'dentist') & (df['rating'] >= 3.5))
        c12 =(df['label_types'].str.contains(pat = 'doctor') & (df['rating'] >= 3.5))
        c13 =(df['label_types'].str.contains(pat = 'drugstore') & (df['rating'] >= 3.5))
        c14 =(df['label_types'].str.contains(pat = 'hospitals') & (df['rating'] >= 3.5))
        c15 =(df['label_types'].str.contains(pat = 'pharmacy') & (df['rating'] >= 3.5))
        c16 =(df['label_types'].str.contains(pat = 'physiotherapist') & (df['rating'] >= 3.5))
        df10 = self.return_df(df,c11,"dentist")
        df11 = self.return_df(df,c12,"doctor")
        df12 = self.return_df(df,c13,"drugstore")
        df13 = self.return_df(df,c14,"hospitals")
        df14 = self.return_df(df,c15,"pharmacy")
        df15 = self.return_df(df,c16,"physiotherapist")

        dfs = [df10, df11, df12, df13, df14, df15]
        df_Medical = reduce(lambda left,right: pd.merge(left,right,on='postcode'), dfs) 

        #Pet care
        c17 =(df['label_types'] =="veterinary care")
        df_Pet = self.return_df(df,c17,"veterinary care")

        #traffic
        traffic = data_OSM.groupby('Postcode',as_index=False).count()
        traffic.rename(columns={"Postcode":'postcode',"_id":"traffic"}, inplace=True)

        return school,df_Pet,df_park,df_diet,df_Medical,df_shopping,traffic

    def get_recommendations(self,input_data):
        postcode_data = pd.DataFrame(["BL0", "BL1", "BL2", "BL3", "BL4", "BL5", "BL6", "BL7", "BL8", "BL9", "M1", "M12", "M13", "M17", "M18", "M19", "M20", "M21", "M22", "M23", "M24", "M25", "M26", "M27", "M28", "M29", "M30", "M31", "M32", "M33", "M34", "M35", "M38", "M41", "M43", "M44", "M45", "M46", "M5", "M7", "M8", "OL1", "OL10", "OL11", "OL12", "OL15", "OL16", "OL2", "OL3", "OL4", "OL5", "OL6", "OL7", "OL8", "OL9", "SK1", "SK14", "SK15", "SK16", "SK2", "SK4", "SK5", "SK6", "SK7", "SK8", "WA13", "WA14", "WA15", "WA3", "WN1", "WN2", "WN3", "WN4", "WN6", "WN7", "B1", "B10", "B11", "B12", "B13", "B14", "B15", "B16", "B17", "B18", "B19", "B2", "B20", "B21", "B22", "B23", "B24", "B25", "B26", "B27", "B28", "B29", "B3", "B30", "B31", "B32", "B33", "B34", "B35", "B36", "B37", "B38", "B39", "B4", "B40", "B41", "B42", "B43", "B44", "B45", "B46", "B47", "B48", "B49", "B5", "B50", "B51", "B52", "B53", "B54", "B55", "B56", "B57", "B58", "B59", "B6", "B60", "B61", "B62", "B63", "B64", "B65", "B66", "B67", "B68", "B69", "B7", "B70", "B71", "B72", "B73", "B74", "B75", "B76", "B77", "B78", "B79", "B8", "B80", "B81", "B82", "B83", "B84", "B85", "B86", "B87", "B88", "B89", "B9", "B90", "B91", "B92", "BS1", "BS10", "BS11", "BS12", "BS13", "BS14", "BS15", "BS16", "BS17", "BS18", "BS19", "BS2", "BS20", "BS21", "BS22", "BS23", "BS24", "BS25", "BS26", "BS27", "BS28", "BS29", "BS3", "BS30", "BS31", "BS32", "BS33", "BS34", "BS35", "BS36", "BS37", "BS38", "BS39", "BS4", "BS40", "BS41", "BS42", "BS43", "BS44", "BS45", "BS46", "BS47", "BS48", "BS49", "BS5", "BS6", "BS7", "BS8", "BS9", "BS98", "BS99", "CB1", "CB2", "CB3", "CB4", "CB5", "BR1", "BR2", "BR3", "BR4", "BR5", "BR6", "BR7", "BR8", "CR0", "CR2", "CR3", "CR4", "CR44", "CR5", "CR6", "CR7", "CR8", "CR9", "CR90", "DA1", "DA14", "DA15", "DA16", "DA17", "DA18", "DA5", "DA6", "DA7", "DA8", "E1", "E10", "E11", "E12", "E13", "E14", "E15", "E16", "E17", "E18", "E1W", "E2", "E20", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "E98", "EC1A", "EC1M", "EC1N", "EC1P", "EC1R", "EC1V", "EC1Y", "EC2A", "EC2M", "EC2N", "EC2P", "EC2R", "EC2V", "EC2Y", "EC3A", "EC3M", "EC3N", "EC3P", "EC3R", "EC3V", "EC4A", "EC4M", "EC4N", "EC4P", "EC4R", "EC4V", "EC4Y", "EN1", "EN2", "EN3", "EN4", "EN5", "EN6", "EN7", "EN8", "EN9", "HA0", "HA1", "HA2", "HA3", "HA4", "HA5", "HA6", "HA7", "HA8", "HA9", "IG1", "IG11", "IG2", "IG3", "IG4", "IG5", "IG6", "IG7", "IG8", "IG9", "KT1", "KT17", "KT18", "KT19", "KT2", "KT22", "KT3", "KT4", "KT5", "KT6", "KT7", "KT8", "KT9", "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N16", "N17", "N18", "N19", "N1C", "N1P", "N2", "N20", "N21", "N22", "N3", "N4", "N5", "N6", "N7", "N8", "N81", "N9", "NW1", "NW10", "NW11", "NW1W", "NW2", "NW26", "NW3", "NW4", "NW5", "NW6", "NW7", "NW8", "NW9", "RM1", "RM10", "RM11", "RM12", "RM13", "RM14", "RM15", "RM2", "RM3", "RM4", "RM5", "RM6", "RM7", "RM8", "RM9", "SE1", "SE10", "SE11", "SE12", "SE13", "SE14", "SE15", "SE16", "SE17", "SE18", "SE19", "SE1P", "SE2", "SE20", "SE21", "SE22", "SE23", "SE24", "SE25", "SE26", "SE27", "SE28", "SE3", "SE4", "SE5", "SE6", "SE7", "SE8", "SE9", "SM1", "SM2", "SM3", "SM4", "SM5", "SM6", "SM7", "SW10", "SW11", "SW12", "SW13", "SW14", "SW15", "SW16", "SW17", "SW18", "SW19", "SW1A", "SW1E", "SW1H", "SW1P", "SW1V", "SW1W", "SW1X", "SW1Y", "SW2", "SW20", "SW3", "SW4", "SW5", "SW6", "SW7", "SW8", "SW9", "SW95", "TN14", "TN16", "TW1", "TW10", "TW11", "TW12", "TW13", "TW14", "TW15", "TW19", "TW2", "TW3", "TW4", "TW5", "TW6", "TW7", "TW8", "TW9", "UB1", "UB10", "UB11", "UB18", "UB2", "UB3", "UB4", "UB5", "UB6", "UB7", "UB8", "UB9", "W10", "W11", "W12", "W13", "W14", "W1A", "W1B", "W1C", "W1D", "W1F", "W1G", "W1H", "W1J", "W1K", "W1S", "W1T", "W1U", "W1W", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9", "WC1A", "WC1B", "WC1E", "WC1H", "WC1N", "WC1R", "WC1V", "WC1X", "WC2A", "WC2B", "WC2E", "WC2H", "WC2N", "WC2R", "WD23", "WD3", "WD6", "BR6", "BR8", "CT1", "CT10", "CT11", "CT12", "CT13", "CT14", "CT15", "CT16", "CT17", "CT18", "CT19", "CT2", "CT20", "CT21", "CT3", "CT4", "CT5", "CT6", "CT7", "CT8", "CT9", "DA1", "DA10", "DA11", "DA12", "DA13", "DA2", "DA3", "DA4", "DA9", "ME1", "ME10", "ME11", "ME12", "ME13", "ME14", "ME15", "ME16", "ME17", "ME18", "ME19", "ME2", "ME20", "ME3", "ME4", "ME5", "ME6", "ME7", "ME8", "ME9", "TN1", "TN10", "TN11", "TN12", "TN13", "TN14", "TN15", "TN16", "TN17", "TN18", "TN2", "TN23", "TN24", "TN25", "TN26", "TN27", "TN28", "TN29", "TN3", "TN30", "TN4", "TN8", "TN9", "BD1", "BD10", "BD11", "BD3", "BD4", "LS1", "LS10", "LS11", "LS12", "LS13", "LS14", "LS15", "LS16", "LS17", "LS18", "LS19", "LS2", "LS20", "LS21", "LS22", "LS23", "LS24", "LS25", "LS26", "LS27", "LS28", "LS29", "LS3", "LS4", "LS5", "LS6", "LS7", "LS8", "LS88", "LS9", "LS98", "LS99", "WF10", "WF12", "WF17", "WF2", "WF3", "L1", "L10", "L11", "L12", "L13", "L14", "L15", "L16", "L17", "L18", "L19", "L2", "L20", "L24", "L25", "L26", "L27", "L28", "L3", "L30", "L36", "L4", "L41", "L43", "L5", "L6", "L67", "L68", "L69", "L7", "L70", "L71", "L73", "L74", "L75", "L8", "L9", "OX1", "OX10", "OX11", "OX12", "OX13", "OX14", "OX15", "OX16", "OX17", "OX18", "OX2", "OX20", "OX25", "OX26", "OX27", "OX28", "OX29", "OX3", "OX33", "OX39", "OX4", "OX44", "OX49", "OX5", "OX6", "OX7", "OX8", "OX9", "RG1", "RG10", "RG18", "RG19", "RG2", "RG26", "RG27", "RG3", "RG30", "RG31", "RG4", "RG5", "RG6", "RG7", "SL0", "SL1", "SL2", "SL3", "SL4", "SL6", "SL95", "SO1", "SO14", "SO15", "SO16", "SO17", "SO18", "SO19", "SO2", "SO3", "SO4", "SO45", "SO9"])
        postcode_data.rename(columns={0:'postcode'}, inplace=True)
        final_dfs= self.data_process(self.school_data,self.google_poi,self.traffic_data)
        full_data = reduce(lambda left,right: pd.merge(left,right,on='postcode',how="left"), final_dfs) 
        full_data=full_data.fillna(0)

        final_df = full_data[["veterinary care"]]
        final_df["education"] = full_data[["school"]]
        final_df["entertainment"] = (full_data["amusement park"]+full_data["rv park"]+full_data["park"])
        final_df["diet"] = (full_data["meal delivery"]+full_data["bakery"]+full_data["bar"]+full_data["cafe"])
        final_df["medical"] = (full_data["dentist"]+full_data["doctor"]+full_data["drugstore"]+full_data["hospitals"]+full_data["pharmacy"]+full_data["physiotherapist"])
        final_df["shopping"] = (full_data["convenience store"]+full_data["department store"]+full_data["shopping mall"]+full_data["supermarket"])
        final_df["traffic"] = full_data[["traffic"]]
        for i in final_df.columns[1:]:
            final_df[i] = final_df[i] / final_df[i].abs().max()

        score_dict = {}
        for index, row in final_df.iterrows():
            score_dict[index] = CosineSimilarity.cos_sim(final_df.iloc[index].astype(float), input_data)



        #sort cities by score and index.
        sorted_scores = sorted(score_dict.items(), key=operator.itemgetter(1), reverse=True)

        counter = 0

        #create an empty results data frame.
        resultDF = pd.DataFrame(columns=("postcode","education","veterinary care","entertainment","diet","medical","shopping","traffic"))#result Data colummns

        #get highest scored 5 cities.
        for i in sorted_scores:
            #print index and score of the city.
            #print(i[0], i[1])
            resultDF = resultDF.append({'postcode': postcode_data.iloc[i[0]]['postcode'],
                                        'education':final_df.iloc[i[0]]['education'],
                                        'veterinary care':final_df.iloc[i[0]]['veterinary care'],
                                        'entertainment':final_df.iloc[i[0]]['entertainment'],
                                        'diet':final_df.iloc[i[0]]['diet'],
                                        'medical':final_df.iloc[i[0]]['medical'],
                                        'shopping':final_df.iloc[i[0]]['shopping'],  
                                        'traffic':final_df.iloc[i[0]]['traffic'],  
                                        'score': i[1]}, ignore_index=True)
            counter += 1

            if counter>4:
                break

        #convert DF to json.
        json_result = json.dumps(resultDF.to_dict('records'))
        return json_result
