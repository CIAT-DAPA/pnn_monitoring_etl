import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Guideline, Objective

class GuidelineT(TransformData):

    def __init__(self, data, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date

        super().__init__(data, self.root_dir, self.actu_date)
        self.guideline_column_name = ExcelColumns.GUIDELINE.value
        self.objective_column_name = ExcelColumns.OBJECTIVE.value
        self.log_error_file = "guideline_error_log.txt"
        self.data_with_error = []

        self.check_columns([self.guideline_column_name, self.objective_column_name])
    

    def obtain_data_from_df(self):

        data_to_save = []
        objective_text = ""

        try:

            objectives_db = self.load.session.query(Objective.id, Objective.name).all()

            if objectives_db:

                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.guideline_column_name]) and row[self.guideline_column_name] and not row[self.guideline_column_name].isspace()):

                        objective_text = row[self.objective_column_name] if pd.notna(row[self.objective_column_name]) and row[self.objective_column_name] else objective_text

                        objective_id = self.get_objective_id(objective_text, objectives_db)

                        if objective_id:

                            normalize_data = self.tools.normalize_text(row[self.guideline_column_name])

                            data = {'normalize': normalize_data, 'original': self.tools.clean_string(row[self.guideline_column_name]),
                                     "objective": objective_id}
                
                            data_to_save.append(data)

                        else:

                            data = {"Fila": index+1, 
                                    'Valor': self.tools.clean_string(row[self.guideline_column_name]),
                                    "Objetivo": self.tools.clean_string(row[self.objective_column_name]),
                                    "Error": "No se encontro el objetivo al cual esta relacionado en la base de datos"}

                            self.data_with_error.append(data)
                        
                    
                
                df_result = pd.DataFrame(data_to_save)

                df_result = df_result.drop_duplicates(subset=['normalize', 'objective'])

                return df_result
            
            else:
                msg_error = f"No hay objetivos en la base de datos con los que relacionar los lineamientos"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)

                return pd.DataFrame(columns=['name'])
    
        except Exception as e:

            msg_error = f"Error al intentar transformar los lineamientos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    
    def obtain_data_from_db(self):

        try: 

            existing_guidelines = self.load.session.query(Guideline.name, Guideline.objective_id, Guideline.sirap_id).all()
            existing_guidelines = set((self.tools.normalize_text(row.name), row.objective_id, row.sirap_id) for row in existing_guidelines)
            return existing_guidelines

        except Exception as e:
            msg_error = f"Error en la tabla Guideline al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
    
    def run_guidelines(self):

        print("\nInicia la transformación de lineamientos")

        existing_guidelines = self.obtain_data_from_db()
        new_guidelines = self.obtain_data_from_df()


        if existing_guidelines is not None and not new_guidelines.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de lineamientos")

            try:

                ac_sirap_id = self.data["id"]
                existing_text_set = {text for text, _, _ in existing_guidelines}
                
                

                for index, row in new_guidelines.iterrows():
                    if (row["normalize"] not in existing_text_set 
                        or not any(text == row["normalize"] and objective_id == row["objective"] and sirap_id == ac_sirap_id
                                    for text, objective_id, sirap_id in existing_guidelines)):

                        guideline = Guideline(name=row["original"], objective_id=row["objective"], sirap_id=ac_sirap_id)
                        self.load.add_to_session(guideline)
                        new_log.append(row["original"])
                        log_data.append(guideline)
                    else:

                        existing_log.append({"Fila": index+1, 
                                             'Valor':row["original"],
                                             "Objetivo": row["objective"],
                                             "Error": f"Este registro ya se encuentra en la base de datos"})
                    
                if log_data:
                    
                    self.load.load_to_db(log_data, self.data["sirap_name"])

                if len(existing_log) > 0 or len(self.data_with_error) > 0:

                    data_with_error = existing_log + self.data_with_error

                    self.tools.generate_csv_with_errors(data_with_error, self.guideline_column_name, self.data["sirap_name"])
                

                msg = f'''Carga de lineamientos exitosa
                Nuevos lineamientos guardados: {len(new_log)}
                Lineamientos ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los lineamientos: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
        else:
            msg_error = f"\nNo hay objetivos a los cuales relacionar los lineamientos, por lo que no se pudo guardar los lineamientos\n"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)



    def get_objective_id(self, text, objectives_db):
        

        normalized_objectives_db = set((self.tools.normalize_text(row.name), row.id) for row in objectives_db)

        text = self.tools.normalize_text(text)
        
        matching_elements = [element for element in normalized_objectives_db if text in element]

        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0



