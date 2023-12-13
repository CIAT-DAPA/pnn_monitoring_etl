import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Action, Guideline

class ActionT(TransformData):

    def __init__(self, data, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date

        super().__init__(data, self.root_dir, self.actu_date)

        self.action_column_name = ExcelColumns.ACTION.value
        self.guideline_column_name = ExcelColumns.GUIDELINE.value
        self.action_indc_column_name = ExcelColumns.ACTION_INDICATOR.value
        
        self.log_error_file = "action_error_log.txt"
        self.data_with_error = []

        self.check_columns([self.action_column_name, self.guideline_column_name, self.action_indc_column_name])
    

    def obtain_data_from_df(self):

        data_to_save = []
        guideline_text = ""

        try:

            guidelines_db = self.load.session.query(Guideline.id, Guideline.name, Guideline.sirap_id).all()

            if guidelines_db:

                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.action_column_name]) and row[self.action_column_name] and not row[self.action_column_name].isspace()):

                        guideline_text = row[self.guideline_column_name] if pd.notna(row[self.guideline_column_name]) and row[self.guideline_column_name] and not row[self.guideline_column_name].isspace() else guideline_text

                        guideline_id = self.get_guideline_id(guideline_text, guidelines_db)

                        if guideline_id:

                            normalize_data = self.tools.normalize_text(row[self.action_column_name])
                            action_indc = row[self.action_indc_column_name] if pd.notna(row[self.action_indc_column_name]) and row[self.action_indc_column_name] else ""


                            data = {'normalize': normalize_data, 'original': self.tools.clean_string(row[self.action_column_name]),
                                     "guideline": guideline_id, "action_indc": self.tools.clean_string(action_indc)}
                
                            data_to_save.append(data)

                        else:

                            data = {"Fila": index+1,
                                    'Valor': self.tools.clean_string(row['original']), "Linea estrategica": row['guideline'], 
                                    "Error": "No se encontro la Linea Estrategica a la cual esta relacionada"}

                            self.data_with_error.append(data)
                        
                    
                
                df_result = pd.DataFrame(data_to_save)

                df_result = df_result.drop_duplicates(subset=['normalize', 'guideline'])

                return df_result
            
            else:
                msg_error = f"No hay lineas estrategicas en la base de datos con los que relacionar las acciones"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)

                return pd.DataFrame(columns=['name'])
    
        except Exception as e:

            msg_error = f"Error al intentar transformar las acciones: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    
    def obtain_data_from_db(self):

        try: 

            existing_actions = self.load.session.query(
                Action.name, 
                Action.guideline_id,
                Guideline.sirap_id
            ).join(
                Guideline, 
                Guideline.id == Action.guideline_id
            ).all()
            existing_actions = set((self.tools.normalize_text(row.name), row.guideline_id, row.sirap_id) for row in existing_actions)
            return existing_actions

        except Exception as e:
            msg_error = f"Error en la tabla Action al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
    
    def run_actions(self):

        print("\nInicia la transformación de las acciones")

        existing_actions = self.obtain_data_from_db()
        new_actions = self.obtain_data_from_df()


        if existing_actions is not None and not new_actions.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de las acciones")

            try:

                ac_sirap_id = self.data["id"]
                existing_text_set = {text for text, _, _ in existing_actions}

                for index, row in new_actions.iterrows():
                    if (row["normalize"] not in existing_text_set 
                        or not any(text == row["normalize"] and guideline_id == row["guideline"] and sirap_id == ac_sirap_id for text, guideline_id, sirap_id in existing_actions)):

                        action = Action(name=row["original"], guideline_id=row["guideline"], action_indc=row["action_indc"])
                        self.load.add_to_session(action)
                        new_log.append(row["original"])
                        log_data.append(action)

                    else:

                        existing_log.append({"Fila": index+1,
                                    'Valor': row['original'], "Linea estrategica": row['guideline'], 
                                    "Error": "Este registro ya se encuentra en la base de datos"})
                    
                if log_data:
                    
                    self.load.load_to_db(log_data, self.data["sirap_name"])

                if len(existing_log) > 0 or len(self.data_with_error) > 0:

                    data_with_error = existing_log + self.data_with_error

                    self.tools.generate_csv_with_errors(data_with_error, self.action_column_name, self.data["sirap_name"])
                

                msg = f'''Carga de las acciones exitosa
                Nuevas acciones guardadas: {len(new_log)}
                Acciones ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar las acciones: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
        else:
            msg_error = f"\nNo hay lineamientos a los cuales relacionar las acciones, por lo que no se pudo guardar las acciones\n"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)



    def get_guideline_id(self, text, guidelines_db):
        
        ac_sirap_id = self.data["id"]
        normalized_guidelines_db = set((self.tools.normalize_text(row.name), row.id, row.sirap_id ) for row in guidelines_db)

        text = self.tools.normalize_text(text)
        
        matching_elements = [element for element in normalized_guidelines_db if text in element and ac_sirap_id == element[2]]

        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0
