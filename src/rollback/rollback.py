import os
import pandas as pd
from database import PostgresConnection
from pnn_monitoring_orm import *
import numpy as np
from sqlalchemy.orm import class_mapper
from tools import Tools
from datetime import datetime


class Rollback():

    def __init__(self, root_dir, id):

        self.workspace_path = os.path.join(root_dir, "workspace")
        
        self.output_path = os.path.join(self.workspace_path, "outputs")
        self.log_path = os.path.join(self.workspace_path, "log")
        self.config_path = os.path.join(root_dir, "config")
        self.config_file = os.path.join(self.config_path, "config_file.csv")
        self.folder_path = os.path.join(self.output_path, id)
        self.error_log_file = "rb_log.txt"

        self.actu_date = datetime.now()
        
        self.tools = Tools(root_dir, self.actu_date)

        self.tools.check_folders([(self.folder_path, True), 
                                  (self.config_path, True), 
                                  (self.workspace_path, False)])
        
        self.tools.check_files([self.config_file])

        os.makedirs(self.log_path, exist_ok=True)

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

        try:

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

                msg = "Se han eliminado los registros con los siguientes IDs: \n{}".format(", ".join(map(str, ids)))
                print(msg)

        except Exception as e:
            msg_error = f'Error al realizar el rollback: {str(e)}'
            self.tools.write_log(msg_error, self.error_log_file)
            print(msg_error)
            return False
        
