import os
from database import PostgresConnection
from extract_data import ExtractData
from transform_data import GuidelineT, ProductT, InstitutionT, MilestoneT, ObjectiveT, ResponsibleT, PeriodT, DetailT, YearT, ActionT, SirapT, ActorT
from load import LoadData
from datetime import datetime
from tools import Tools

class ETLMaster():

    def __init__(self,root_dir):
        
        self.root_dir = root_dir
        self.workspace_path = os.path.join(self.root_dir, "workspace")
        self.files_to_import_path = os.path.join(self.workspace_path, "import")
        self.output_path = os.path.join(self.workspace_path, "outputs")
        self.config_path = os.path.join(root_dir, "config")
        self.config_file = os.path.join(self.config_path, "config_file.csv")
        self.log_path = os.path.join(self.workspace_path, "log")
        self.connection = None
        
        self.actu_date = datetime.now()

        self.tools = Tools(self.root_dir, self.actu_date)

        #Check folders
        self.tools.check_folders([(self.workspace_path, False), 
                                  (self.files_to_import_path, True), 
                                  (self.config_path, True)])
        
        self.tools.check_files([self.config_file])
        

        os.makedirs(self.workspace_path, exist_ok=True)
        os.makedirs(self.files_to_import_path, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(self.config_path, exist_ok=True)
        os.makedirs(self.log_path, exist_ok=True)

        

    def database_connection(self):        
        db = PostgresConnection(self.config_path)
        db.connect()
        return db

    def extract(self, connection):
        raw_data = ExtractData(self.files_to_import_path, connection, self.config_path, self.actu_date)
        raw_data.read_data()
        return raw_data

    def run_etl(self):

        self.connection = self.database_connection()

        load = LoadData(self.connection.session, self.root_dir, self.actu_date)
        
        sirap = SirapT(load)
        sirap.run_sirap()

        raw_data = self.extract(self.connection)
        for data in raw_data.dfs:

            print(f'------------------------------   Iniciando el proceso para el SIRAP: {data["sirap_name"]}  -------------------------- \n\n')
            objective = ObjectiveT(data, load)
            objective.run_objective()

            product = ProductT(data, load)
            product.run_products()

            period = PeriodT(data, load)
            period.run_periods()

            guideline = GuidelineT(data, load)
            guideline.run_guidelines()

            action = ActionT(data, load)
            action.run_actions()

            institution = InstitutionT(data, load)
            institution.run_institution()

            milestone = MilestoneT(data, load)
            milestone.run_milestone()

            year = YearT(data, load)
            year.run_year()

            detail = DetailT(data, load)
            detail.run_detail()

            responsible = ResponsibleT(data, load)
            responsible.run_responsible()

            actor = ActorT(data, load)
            actor.run_actor()

            print(f'------------------------------   Finalizando el proceso para el SIRAP: {data["sirap_name"]}  -------------------------- \n\n')

        self.connection.disconnect()
        print("Proceso ETL completado.")


