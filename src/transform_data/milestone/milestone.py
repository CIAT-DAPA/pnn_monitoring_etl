import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Milestone, Action

class MilestoneT(TransformData):

    def __init__(self,data, load):
        super().__init__(data)
        self.milestone_column_name = ExcelColumns.MILESTONE.value
        self.action_column_name = ExcelColumns.ACTION.value
        self.prod_ind_column_name = ExcelColumns.PRODUCT_INDICATOR.value
        self.load = load
        self.log_error_file = "milestone_error_log.txt"

        self.check_columns([self.milestone_column_name, self.action_column_name, self.prod_ind_column_name])
    

    def obtain_data_from_df(self):

        data_to_save = []
        action_text = ""

        try:

            actions_db = self.load.session.query(Action.id, Action.name).all()

            if actions_db:

                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.milestone_column_name]) and row[self.milestone_column_name]):

                        action_text = row[self.action_column_name] if pd.notna(row[self.action_column_name]) and row[self.action_column_name] else actions_db

                        action_id = self.get_action_id(action_text, actions_db)

                        if action_id:

                            normalize_data = self.tools.normalize_text(row[self.milestone_column_name])

                            data = {'normalize': normalize_data, 'original': row[self.milestone_column_name], "action": action_id, "prod_ind": row[self.prod_ind_column_name]}
                
                            data_to_save.append(data)
                        
                    
                
                df_result = pd.DataFrame(data_to_save)

                df_result = df_result.drop_duplicates(subset='normalize')

                return df_result
            
            else:
                msg_error = f"\nNo hay acciones en la base de datos con los que relacionar los hitos"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)

                return pd.DataFrame(columns=['name'])
    
        except Exception as e:

            msg_error = f"\nError al intentar transformar los hitos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    
    def obtain_data_from_db(self):

        try: 

            existing_products = self.load.session.query(Milestone.name, Milestone.action_id).all()
            existing_products = set((self.tools.normalize_text(row.name), row.action_id) for row in existing_products)
            return existing_products

        except Exception as e:
            msg_error = f"\nError en la tabla Milestone al intentar obtener los datos: {str(e)}"
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

            print("\nInicia la carga de los hitos")

            try:

                existing_text_set = {text for text, _ in existing_data}
                
                

                for index, row in new_data.iterrows():
                    if (row["normalize"] not in existing_text_set 
                        or any(text == row["normalize"] and action_id != row["action"] for text, action_id, _ in existing_data)):

                        milestone = Milestone(name=row["original"], action_id=row["action"], producto_indicator=row["prod_ind"])
                        self.load.add_to_session(milestone)
                        new_log.append(row["original"])
                        log_data.append(milestone)
                    else:

                        existing_log.append(row["original"])
                    
                if log_data:
                    
                    self.load.load_to_db(log_data)
                

                msg = f'''\nCarga de los hitos exitosa
                Nuevos hitos guardados: {len(new_log)}
                Hitos ya existentes en la base de datos: {len(existing_log)}'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"\nError al guardar los hitos: {str(e)}"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
        else:
            msg_error = f"\nNo hay acciones a los cuales relacionar los hitos, por lo que no se pudo guardar los hitos"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)



    def get_action_id(self, text, action_db):
        

        normalized_action_db = set((self.tools.normalize_text(row.name), row.id) for row in action_db)

        text = self.tools.normalize_text(text)
        
        matching_elements = [element for element in normalized_action_db if text in element]

        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0



