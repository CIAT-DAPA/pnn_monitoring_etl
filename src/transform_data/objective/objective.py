import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Objective

class ObjectiveT(TransformData):

    def __init__(self, data, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date

        super().__init__(data, self.root_dir, self.actu_date)
        self.objective_column_name = ExcelColumns.OBJECTIVE.value
        self.log_error_file = "objective_error_log.txt"

        self.check_columns([self.objective_column_name])
    

    def obtain_data_from_df(self):

        data_to_save = []

        try:

            for index, row in self.data["data"].iterrows():
                
                if(pd.notna(row[self.objective_column_name]) and row[self.objective_column_name]):

                    normalize_data = self.tools.normalize_text(row[self.objective_column_name])
                    data = {'normalize': normalize_data, 'original': self.tools.clean_string(row[self.objective_column_name])}
                    data_to_save.append(data)
                    
            df_result = pd.DataFrame(data_to_save)

            df_result = df_result.drop_duplicates(subset='normalize')

            return df_result
            
    
        except Exception as e:

            msg_error = f"Error al intentar transformar los objetivos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    
    def obtain_data_from_db(self):

        try: 

            existing_objectives = self.load.session.query(Objective.name).all()
            existing_objectives = set(self.tools.normalize_text(row[0]) for row in existing_objectives)
            return existing_objectives

        except Exception as e:
            msg_error = f"Error en la tabla Objetive al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
    
    def run_objective(self):

        print("\nInicia la transformación de los objetivos...")

        existing_data = self.obtain_data_from_db()
        new_data = self.obtain_data_from_df()


        if existing_data is not None and not new_data.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de los objetivos...")

            try:

                for index, row in new_data.iterrows():
                    if row["normalize"] not in existing_data:

                        objective = Objective(name=row["original"])
                        self.load.add_to_session(objective)
                        new_log.append(row["original"])
                        log_data.append(objective)
                    else:

                        existing_log.append({"Fila": index+1, 
                                             'Valor':row["original"],
                                             "Error": f"Este objetivo ya se encuentra en la base de datos"})

                if log_data:
                    
                    self.load.load_to_db(log_data, self.data["sirap_name"])

                if len(existing_log) > 0:
                    self.tools.generate_csv_with_errors(existing_log, self.objective_column_name, self.data["sirap_name"])

                msg = f'''Carga de objetivos exitosa
                Nuevos objetivos guardados: {len(new_log)}
                Objetivos ya existentes en la base de datos: {len(existing_log)}'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los objetivos: {str(e)}"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
        else:
            msg_error = f"Error con los objetivos"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

