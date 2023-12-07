import os
import pandas as pd


class Rollback():

    def __init__(self, parent_dir, id):
        
        self.output_path = os.path.join(parent_dir, "outputs")
        self.folder_path = os.path.join(self.output_path, id)

        self.encoding = "ISO-8859-1"

        self.models = {
            "action": "Action",
            "actor": "Actor",
            "guideline": "Guideline",
            "detail": "Detail",
            "institution": "Institution",
            "milestone": "Milestone",
            "objective": "Objective",
            "period": "Period",
            "product": "Product",
            "responsible": "Responsible",
            "time": "Time",
            "sirap": "Sirap",
            "year": "Year"
        }

        self.run_rollback()


    def read_csvs(self, filter):

        dfs = []

        if os.path.isdir(self.folder_path):

            files = os.listdir(self.folder_path)

            filtered_files = [file for file in files if f"_{filter}_" in file]

            for df in filtered_files:

                dataframe = pd.read_csv(os.path.join(self.folder_path, df), encoding=self.encoding)

                dfs.append(dataframe)
        
        return dfs

    
    def run_rollback(self):


        responsible = self.read_csvs(self.models["responsible"])
        actor = self.read_csvs(self.models["actor"])
        time = self.read_csvs(self.models["time"])
        detail = self.read_csvs(self.models["detail"])
        milestone = self.read_csvs(self.models["milestone"])
        action = self.read_csvs(self.models["action"])
        guideline = self.read_csvs(self.models["guideline"])
        institution = self.read_csvs(self.models["institution"])
        period = self.read_csvs(self.models["period"])
        product = self.read_csvs(self.models["product"])
        year = self.read_csvs(self.models["year"])
        objective = self.read_csvs(self.models["objective"])
        sirap = self.read_csvs(self.models["sirap"])
            
        
