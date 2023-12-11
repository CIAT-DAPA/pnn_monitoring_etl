import os
import pandas as pd
from database import PostgresConnection
from pnn_monitoring_orm import *
import numpy as np
from sqlalchemy.orm import class_mapper


class Rollback():

    def __init__(self, root_dir, id):
        
        self.output_path = os.path.join(root_dir, "outputs")
        self.config_path = os.path.join(root_dir, "config")
        self.folder_path = os.path.join(self.output_path, id)

        self.encoding = "ISO-8859-1"

        self.models = {
            "responsible": "Responsible",
            "actor": "Actor",
            "time": "Time",
            "detail": "Detail",
            "milestone": "Milestone",
            "action": "Action",
            "guideline": "Guideline",
            "institution": "Institution",
            "period": "Period",
            "product": "Product",
            "year": "Year",
            "objective": "Objective",
            "sirap": "Sirap",
        }

        self.run_rollback()

    def database_connection(self):        
        db = PostgresConnection(self.config_path)
        db.connect()
        return db

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

        session = self.database_connection().session

        for key, element in self.models.items():

            dfs = self.read_csvs(element)

            ids = np.concatenate([df["id"].to_numpy() for df in dfs]).tolist()

            model = None

            for nombre, objeto in globals().items():
                if isinstance(objeto, type) and issubclass(objeto, Base) and objeto != Base and nombre == element:
                    model = objeto

            print(f"\n\n---------------------  Borrando datos de la tabla: {element} -------------------------")
            session.query(model).filter(model.id.in_(ids)).delete(synchronize_session=False)

            session.commit()
        
