import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Institution

class InstitutionT(TransformData):

    def __init__(self, data, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date

        super().__init__(data, self.root_dir, self.actu_date)

        self.column_name_actor = ExcelColumns.ACTOR.value 
        self.column_name_responsible = ExcelColumns.RESPONSIBLE.value
        self.log_error_file = "institution_error_log.txt"
        self.check_columns([self.column_name_actor, self.column_name_responsible])

    def obtain_data_from_df(self):

        data_to_save = []

        try:

            for index, row in self.data["data"].iterrows():
                if(pd.notna(row[self.column_name_actor])):
                    institutions=row[self.column_name_actor]
                    institutions=institutions.replace(" - ", ",")
                    institutions=institutions.replace("\n", ",").split(",")
                    institutions=[institution for institution in institutions if institution.strip()]
                    institutions=[institution.strip() for institution in institutions]
                    for data in institutions:
                        data = {'normalize': self.tools.normalize_text(data), 'original': self.tools.clean_string(data)}
                        data_to_save.append(data)

                if(pd.notna(row[self.column_name_responsible])):
                    institutions=row[self.column_name_responsible]
                    institutions=institutions.replace(" - ", ",")
                    institutions=institutions.replace("\n", ",").split(",")
                    institutions=[institution for institution in institutions if institution.strip()]
                    institutions=[institution.strip() for institution in institutions]
                    for data in institutions:
                        data = {'normalize': self.tools.normalize_text(data), 'original': self.tools.clean_string(data)}
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

        print("\nInicia la transformación de instituciones")

        existing_institution = self.obtain_data_from_db()
        new_institution = self.obtain_data_from_df()

        print("Finalizada la transformación de instituciones")

        if existing_institution is not None and not new_institution.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de instituciones")

            try:

                for index, row in new_institution.iterrows():
                    if row["normalize"] not in existing_institution:

                        institution = Institution(name=row["original"], ext_id='')
                        self.load.add_to_session(institution)
                        new_log.append(row["original"])
                        log_data.append(institution)
                    else:
                        existing_log.append({"Fila": index, 
                                             'Valor':row["original"],
                                             "Error": f"Esta institución ya se encuentra en la base de datos"})

                if log_data:
                    
                    self.load.load_to_db(log_data, self.data["sirap_name"])

                if len(existing_log) > 0:
                    self.tools.generate_csv_with_errors(existing_log, "Instituciones", self.data["sirap_name"])

                msg = f'''Carga de instituciones exitosa
                Nuevas Instituciones guardados: {len(new_log)}
                Instituciones ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar las instituciones: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)