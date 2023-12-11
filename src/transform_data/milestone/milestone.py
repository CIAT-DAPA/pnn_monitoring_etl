import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Milestone, Action, Guideline

class MilestoneT(TransformData):

    def __init__(self, data, load, root_dir):
        super().__init__(data, root_dir)
        self.milestone_column_name = ExcelColumns.MILESTONE.value
        self.action_column_name = ExcelColumns.ACTION.value
        self.prod_ind_column_name = ExcelColumns.PRODUCT_INDICATOR.value
        self.prod_obs_column_name = ExcelColumns.OBSERVATION.value
        self.load = load
        self.log_error_file = "milestone_error_log.txt"
        self.data_with_error = []

        self.check_columns([self.milestone_column_name, self.action_column_name, self.prod_ind_column_name, self.prod_obs_column_name])
    

    def obtain_data_from_df(self):

        data_to_save = []
        action_text = ""

        try:

            actions_db = self.load.session.query(Action.id, Action.name, Action.guideline_id, Guideline.sirap_id).join(Guideline, Guideline.id == Action.guideline_id).all()

            if actions_db:

                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.milestone_column_name]) and row[self.milestone_column_name] and not row[self.milestone_column_name].isspace()):

                        action_text = row[self.action_column_name] if pd.notna(row[self.action_column_name]) and row[self.action_column_name] else action_text

                        action_id = self.get_action_id(action_text, actions_db)
                        if action_id:

                            prod_ind = row[self.prod_ind_column_name] if row[self.prod_ind_column_name] and pd.notna(row[self.prod_ind_column_name]) else ""
                            obs = row[self.prod_obs_column_name] if row[self.prod_obs_column_name] and pd.notna(row[self.prod_obs_column_name]) else ""

                            normalize_data = self.tools.normalize_text(row[self.milestone_column_name])

                            data = {'normalize': normalize_data, 'original': row[self.milestone_column_name], 
                                    "action": action_id, "prod_ind": prod_ind, "obs": obs}
                            
                
                            data_to_save.append(data)
                        
                        else:

                            data = {"Columna": self.milestone_column_name, "Fila": index+1, 
                                             'Valor':row[self.milestone_column_name],
                                             "Acción": action_text,
                                             "Error": f"No se encontro la acción a la cual esta relacionado"}

                            self.data_with_error.append(data)
                        
                    
                
                df_result = pd.DataFrame(data_to_save)

                df_result = df_result.drop_duplicates(subset='normalize')

                return df_result
            
            else:
                msg_error = f"No hay acciones en la base de datos con los que relacionar los hitos"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)

                return pd.DataFrame(columns=['name'])
    
        except Exception as e:

            msg_error = f"Error al intentar transformar los hitos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    
    def obtain_data_from_db(self):

        try: 

            existing_milestones = self.load.session.query(
                Milestone.name, 
                Milestone.action_id, 
                Guideline.sirap_id
            ).join(
                Action,
                Action.id == Milestone.action_id
            ).join(
                Guideline,
                Guideline.id == Action.guideline_id
            ).all()
            existing_milestones = set((self.tools.normalize_text(row.name), row.action_id, row.sirap_id) for row in existing_milestones)
            return existing_milestones

        except Exception as e:
            msg_error = f"Error en la tabla Milestone al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
    
    def run_milestone(self):

        print("\nInicia la transformación de los hitos")

        existing_data = self.obtain_data_from_db()
        new_data = self.obtain_data_from_df()


        if existing_data is not None and not new_data.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de los hitos")

            try:

                existing_text_set = {text for text, _, _ in existing_data}
                
                ac_sirap_id = self.data["id"]

                for index, row in new_data.iterrows():
                    if (row["normalize"] not in existing_text_set 
                        or not any(text == row["normalize"] and action_id == row["action"] and sirap_id == ac_sirap_id
                                    for text, action_id, sirap_id in existing_data)):

                        milestone = Milestone(name=row["original"], action_id=row["action"], product_indc=row["prod_ind"], obs=row["obs"]  )
                        self.load.add_to_session(milestone)
                        new_log.append(row["original"])
                        log_data.append(milestone)
                    else:

                        existing_log.append({"Columna": self.milestone_column_name, "Fila": index+1, 
                                             'Valor':row["original"],
                                             "Acción": row["action"],
                                             "Error": f"Este registro ya se encuentra en la base de datos"})
                    
                if log_data:
                    
                    self.load.load_to_db(log_data, self.data["sirap_name"])


                if len(existing_log) > 0 or len(self.data_with_error) > 0:

                    data_with_error = existing_log + self.data_with_error

                    self.tools.generate_csv_with_errors(data_with_error, self.milestone_column_name, self.data["sirap_name"])
                

                msg = f'''Carga de los hitos exitosa
                Nuevos hitos guardados: {len(new_log)}
                Hitos ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los hitos: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
        else:
            msg_error = f"No hay acciones a los cuales relacionar los hitos, por lo que no se pudo guardar los hitos\n"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)



    def get_action_id(self, text, action_db):
        
        ac_sirap_id = self.data["id"]
        normalized_action_db = set((self.tools.normalize_text(row.name), row.id, row.sirap_id) for row in action_db)

        text = self.tools.normalize_text(text)
        
        matching_elements = [element for element in normalized_action_db if text in element and ac_sirap_id == element[2]]

        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0



