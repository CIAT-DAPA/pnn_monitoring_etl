import os
from database import PostgresConnection
from extract_data import ExtractData
from transform_data import GuidelineT, ProductT, InstitutionT, MilestoneT, ObjectiveT, ResponsibleT, PeriodT, DetailT, YearT, ActionT, SirapT, ActorT
from load import LoadData
import shutil

class ETLMaster():

    def __init__(self,root_dir):
        
        self.root_dir = root_dir
        self.files_to_import_path = os.path.join(root_dir, "import")
        self.output_path = os.path.join(root_dir, "outputs")
        self.config_path = os.path.join(root_dir, "config")
        self.log_path = os.path.join(root_dir, "log")
        self.connection = None
        if os.path.isdir(self.log_path):
            shutil.rmtree(self.log_path)

        os.makedirs(self.files_to_import_path, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(self.config_path, exist_ok=True)
        os.makedirs(self.log_path, exist_ok=True)

        

    def database_connection(self):        
        db = PostgresConnection(self.config_path)
        db.connect()
        return db

    def extract(self, connection):
        raw_data = ExtractData(self.files_to_import_path, connection, self.config_path)
        raw_data.read_data()
        return raw_data

    def run_etl(self):

        self.connection = self.database_connection()

        load = LoadData(self.connection.session, self.root_dir)
        
        sirap = SirapT(load, self.root_dir)
        sirap.run_sirap()

        raw_data = self.extract(self.connection)
        for data in raw_data.dfs:

            print(f'------------------------------   Iniciando el proceso para el SIRAP: {data["sirap_name"]}  -------------------------- \n\n')
            objective = ObjectiveT(data, load, self.root_dir)
            objective.run_objective()

            product = ProductT(data, load, self.root_dir)
            product.run_products()

            period = PeriodT(data, load, self.root_dir)
            period.run_periods()

            guideline = GuidelineT(data, load, self.root_dir)
            guideline.run_guidelines()

            action = ActionT(data, load, self.root_dir)
            action.run_actions()

            institution = InstitutionT(data, load, self.root_dir)
            institution.run_institution()

            milestone = MilestoneT(data, load, self.root_dir)
            milestone.run_milestone()

            year = YearT(data, load, self.root_dir)
            year.run_year()

            detail = DetailT(data, load, self.root_dir)
            detail.run_detail()

            responsible = ResponsibleT(data, load, self.root_dir)
            responsible.run_responsible()

            actor = ActorT(data, load, self.root_dir)
            actor.run_actor()

            print(f'------------------------------   Finalizando el proceso para el SIRAP: {data["sirap_name"]}  -------------------------- \n\n')

        self.connection.disconnect()
        print("Proceso ETL completado.")


