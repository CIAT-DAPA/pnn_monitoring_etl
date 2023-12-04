import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Responsible, Institution, Detail

class ResponsibleT(TransformData):

    def __init__(self,data, load):
        super().__init__(data)
        self.column_name_responsible = ExcelColumns.RESPONSIBLE.value 
        self.column_name_detail = ExcelColumns.DETAIL.value 
        self.load = load
        self.log_error_file = "responsible_error_log.txt"
        self.check_columns([self.column_name_responsible, self.column_name_detail])
    
    def obtain_data_from_df(self):

        data_to_save = []
        institution_text = ""
        detail_text = ""
        try:
            institutions_db = self.load.session.query(Institution.id, Institution.name).all()
            details_db = self.load.session.query(Detail.id, Detail.name).all()

            if institutions_db and details_db:
                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.column_name_responsible])):
                        responsibles=row[self.column_name_responsible]
                        responsibles=responsibles.replace("-", ",").split(",")
                        detail = row[self.column_name_detail]
                        detail_id = self.get_detail_id(detail, details_db)
                        for data in responsibles:
                            data = {'normalize': self.tools.normalize_text(data), 'original': data}
                            institution_text = data['original']
                            intitution_id = self.get_institution_id(institution_text, institutions_db)
                            data_to_save.append({'institution_id': intitution_id, 'detail_id': detail_id})

                df_result = pd.DataFrame(data_to_save)
                df_result = df_result.drop_duplicates(subset=['institution_id', 'detail_id'])
                return df_result
            
            else:
                if not institutions_db:
                    msg_error = f"No hay instituciones en la base de datos con los que relacionar los responsables"
                else:
                    msg_error = f"No hay detalles en la base de datos con los que relacionar los responsables"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)

                return pd.DataFrame(columns=['name'])
    
        except Exception as e:

            msg_error = f"Error al intentar transformar los responsables: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])
    
    def obtain_data_from_db(self):

        try: 

            existing_responsible = self.load.session.query(Responsible.institution_id, Responsible.detail_id).all()
            return existing_responsible

        except Exception as e:
            msg_error = f"Error en la tabla Responsible al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None


    def get_institution_id(self, text, institution_db):
        normalized_institution_db = set((self.tools.normalize_text(row.name), row.id) for row in institution_db)
        text = self.tools.normalize_text(text)       
        matching_elements = [element for element in normalized_institution_db if text in element]
        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0
    
    def get_detail_id(self, text, detail_db):
        normalized_detail_db = set((self.tools.normalize_text(row.name), row.id) for row in detail_db)
        text = self.tools.normalize_text(text)       
        matching_elements = [element for element in normalized_detail_db if text in element]
        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0
        
    def run_responsible(self):

        print("\nInicia la transformación de Responsible")

        existing_responsible = self.obtain_data_from_db()
        new_responsible = self.obtain_data_from_df()

        print("Finalizada la transformación de Responsible")

        if existing_responsible is not None and not new_responsible.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de Responsible")

            try:
                for index, row in new_responsible.iterrows():
                    if (row["institution_id"], row["detail_id"]) not in existing_responsible:
                        responsible = Responsible(institution_id=int(row["institution_id"]), detail_id=int(row["detail_id"]))
                        self.load.add_to_session(responsible)
                        new_log.append(row["institution_id"], row["detail_id"])
                        log_data.append(responsible)
                    else:
                        existing_log.append(row["institution_id"])

                if log_data:
                    self.load.load_to_db(log_data)

                msg = f'''Carga de Responsible exitosa
                Nuevas Responsible guardados: {len(new_log)}
                Responsible ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar las Responsible: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)