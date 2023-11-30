import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
import unicodedata
from models.institution import Institution

class InstitutionT(TransformData):

    def __init__(self,data, load):
        super().__init__(data)
        self.column_name_actor = ExcelColumns.ACTOR.value 
        self.column_name_responsible = ExcelColumns.RESPONSIBLE.value 
        self.load = load
        self.log_error_file = "institution_error_log.txt"
        self.check_columns([self.column_name_actor, self.column_name_responsible])

    def obtain_data_from_df(self):

        data_to_save = []

        try:

            for index, row in self.data["data"].iterrows():
                if(pd.notna(row[self.column_name_actor])):
                    institutions=row[self.column_name_actor]
                    institutions=institutions.replace("-", ",").split(",")
                    for data in institutions:
                        data = {'normalize': self.tools.normalize_text(data), 'original': data}
                        data_to_save.append(data)

                if(pd.notna(row[self.column_name_responsible])):
                    institutions=row[self.column_name_responsible]
                    institutions=institutions.replace("-", ",").split(",")
                    for data in institutions:
                        data = {'normalize': self.tools.normalize_text(data), 'original': data}
                        data_to_save.append(data)

            df_result = pd.DataFrame(data_to_save)
            df_result = df_result.drop_duplicates(subset='normalize')
            return df_result
    
        except Exception as e:

            msg_error = f"Error al intentar transformar las instituciones: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    def obtain_data_from_db(self):

        try: 

            existing_institution = self.load.session.query(Institution.name).all()
            existing_institution = set(self.tools.normalize_text(row[0]) for row in existing_institution)
            return existing_institution

        except Exception as e:
            msg_error = f"Error en la tabla Instituciones al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
        
    def run_institution(self):

        print("Inicia la transformación de instituciones")

        existing_institution = self.obtain_data_from_db()
        new_institution = self.obtain_data_from_df()

        print("Finalizada la transformación de instituciones")

        if existing_institution is not None and not new_institution.empty:

            new_log = []
            existing_log = []

            print("Inicia la carga de instituciones")

            try:

                for index, row in new_institution.iterrows():
                    if row["normalize"] not in existing_institution:

                        institution = Institution(name=row["original"], ext_id='')
                        self.load.add_to_session(institution)
                        new_log.append(row["original"])
                    else:
                        existing_log.append(row["original"])

                self.load.load_to_db()

                msg = f'''Carga de instituciones exitosa
                Nuevas Instituciones guardados: {len(new_log)}
                Instituciones ya existentes en la base de datos: {len(existing_log)}'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar las instituciones: {str(e)}"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)