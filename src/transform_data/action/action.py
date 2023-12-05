import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Action, Guideline

class ActionT(TransformData):

    def __init__(self,data, load):
        super().__init__(data)
        self.action_column_name = ExcelColumns.ACTION.value
        self.guideline_column_name = ExcelColumns.GUIDELINE.value
        self.action_indc_column_name = ExcelColumns.ACTION_INDICATOR.value
        self.load = load
        self.log_error_file = "action_error_log.txt"
        self.data_with_error = []

        self.check_columns([self.action_column_name, self.guideline_column_name, self.action_indc_column_name])
    

    def obtain_data_from_df(self):

        data_to_save = []
        guideline_text = ""

        try:

            guidelines_db = self.load.session.query(Guideline.id, Guideline.name).all()

            if guidelines_db:

                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.action_column_name]) and row[self.action_column_name] and not row[self.action_column_name].isspace()):

                        guideline_text = row[self.guideline_column_name] if pd.notna(row[self.guideline_column_name]) and row[self.guideline_column_name] and not row[self.guideline_column_name].isspace() else guideline_text

                        guideline_id = self.get_guideline_id(guideline_text, guidelines_db)

                        if guideline_id:

                            normalize_data = self.tools.normalize_text(row[self.action_column_name])
                            action_indc = row[self.action_indc_column_name] if pd.notna(row[self.action_indc_column_name]) and row[self.action_indc_column_name] else ""


                            data = {'normalize': normalize_data, 'original': row[self.action_column_name], "guideline": guideline_id, "action_indc": action_indc}
                
                            data_to_save.append(data)

                        else:

                            data = {'original': row[self.action_column_name], "guideline": row[self.guideline_column_name], 
                                    "row": index, "column": self.action_column_name, "error": "No se encontro la lineamiento al cual esta relacionada"}

                            self.data_with_error.append(data)
                        
                    
                
                df_result = pd.DataFrame(data_to_save)

                df_result = df_result.drop_duplicates(subset=['normalize', 'guideline'])

                return df_result
            
            else:
                msg_error = f"No hay lineamientos en la base de datos con los que relacionar las acciones"
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

            existing_products = self.load.session.query(Action.name, Action.guideline_id, Action.action_indc).all()
            existing_products = set((self.tools.normalize_text(row.name), row.guideline_id, row.action_indc) for row in existing_products)
            return existing_products

        except Exception as e:
            msg_error = f"Error en la tabla Action al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
    
    def run_actions(self):

        print("\nInicia la transformación de las acciones")

        existing_products = self.obtain_data_from_db()
        new_products = self.obtain_data_from_df()


        if existing_products is not None and not new_products.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de las acciones")

            try:

                existing_text_set = {text for text, _, _ in existing_products}

                for index, row in new_products.iterrows():
                    if (row["normalize"] not in existing_text_set 
                        or any(text == row["normalize"] and guideline_id != row["guideline"] for text, guideline_id, _ in existing_products)):

                        action = Action(name=row["original"], guideline_id=row["guideline"], action_indc=row["action_indc"])
                        self.load.add_to_session(action)
                        new_log.append(row["original"])
                        log_data.append(action)

                    else:

                        existing_log.append(row["original"])
                    
                if log_data:
                    
                    self.load.load_to_db(log_data)
                

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
        

        normalized_guidelines_db = set((self.tools.normalize_text(row.name), row.id) for row in guidelines_db)

        text = self.tools.normalize_text(text)
        
        matching_elements = [element for element in normalized_guidelines_db if text in element]

        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0