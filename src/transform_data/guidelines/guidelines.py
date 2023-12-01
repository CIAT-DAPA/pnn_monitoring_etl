import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Guideline, Objective

class GuidelineT(TransformData):

    def __init__(self,data, load):
        super().__init__(data)
        self.guideline_column_name = ExcelColumns.GUIDELINE.value
        self.objective_column_name = ExcelColumns.OBJECTIVE.value
        self.load = load
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
                    if(pd.notna(row[self.guideline_column_name]) and row[self.guideline_column_name]):

                        objective_text = row[self.objective_column_name] if pd.notna(row[self.objective_column_name]) and row[self.objective_column_name] else objective_text

                        objective_id = self.get_objective_id(objective_text, objectives_db)

                        if objective_id:

                            normalize_data = self.tools.normalize_text(row[self.guideline_column_name])

                            data = {'normalize': normalize_data, 'original': row[self.guideline_column_name], "objective": objective_id}
                
                            data_to_save.append(data)

                        else:

                            data = {'original': row[self.milestone_column_name], "prod_ind": row[self.prod_ind_column_name], 
                                    "row": index, "column": self.milestone_column_name, "error": "No se encontro la acción a la cual esta relacionado"}

                            self.data_with_error.append(data)
                        
                    
                
                df_result = pd.DataFrame(data_to_save)

                df_result = df_result.drop_duplicates(subset='normalize')

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

            existing_products = self.load.session.query(Guideline.name, Guideline.objective_id, Guideline.sirap_id).all()
            existing_products = set((self.tools.normalize_text(row.name), row.objective_id, row.sirap_id) for row in existing_products)
            return existing_products

        except Exception as e:
            msg_error = f"Error en la tabla Guideline al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
    
    def run_guidelines(self):

        print("\nInicia la transformación de lineamientos")

        existing_products = self.obtain_data_from_db()
        new_products = self.obtain_data_from_df()


        if existing_products is not None and not new_products.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de lineamientos")

            try:

                ac_sirap_id = self.data["id"]
                existing_text_set = {text for text, _, _ in existing_products}
                
                

                for index, row in new_products.iterrows():
                    if (row["normalize"] not in existing_text_set 
                        or any(text == row["normalize"] and objective_id != row["objective"] for text, objective_id, _ in existing_products) 
                        or any(text == row["normalize"] and objective_id == row["objective"] and sirap_id != ac_sirap_id 
                               for text, objective_id, sirap_id in existing_products) ):

                        guideline = Guideline(name=row["original"], objective_id=row["objective"], sirap_id=ac_sirap_id)
                        self.load.add_to_session(guideline)
                        new_log.append(row["original"])
                        log_data.append(guideline)
                    else:

                        existing_log.append(row["original"])
                    
                if log_data:
                    
                    self.load.load_to_db(log_data)
                

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



