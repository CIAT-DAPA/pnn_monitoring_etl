import os
import pandas as pd
from transform_data import TransformData

class Guideline(TransformData):

    def __init__(self,data):
        super().__init__(data)
    

    def obtain_data(self):
        print(self.data["data"].iloc[0])
        print(self.data["data"].iloc[1])
        #for indice, row in self.data["data"].iterrows():
            #print(row)