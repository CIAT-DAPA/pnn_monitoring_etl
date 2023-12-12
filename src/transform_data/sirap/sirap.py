import pandas as pd
from tools import Tools
from enums import ExcelColumns
from pnn_monitoring_orm import Sirap
import os

class SirapT():

    def __init__(self, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date
        self.tools = Tools(self.root_dir, self.actu_date)
        
        self.log_error_file = "sirap_error_log.txt"
        self.script_dir = os.path.join(self.root_dir, "workspace")

    def obtain_data_from_df(self):
        data_to_save = []

        try:
            import_path = os.path.join(self.script_dir, "import")
            excel_files = [file for file in os.listdir(import_path) if file.endswith(".xlsx")]

            for file in excel_files:
                file= os.path.splitext(file)[0]
                data = self.tools.normalize_text(file)
                sirap_to_save = {'original': file, 'normalized': data}
                data_to_save.append(sirap_to_save)

            df_result = pd.DataFrame(data_to_save)

            df_result = df_result.drop_duplicates(subset='normalized')

            return df_result
        
        except Exception as e:
    
            msg_error = f"Error al intentar transformar los SIRAP: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['original'])
        

    def obtain_data_from_db(self):

        try: 

            existing_sirap = self.load.session.query(Sirap.name).all()
            existing_sirap = set(self.tools.normalize_text(str(row[0])) for row in existing_sirap)
            return existing_sirap

        except Exception as e:
            msg_error = f"Error en la tabla SIRAP al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
        
        
    def run_sirap(self):

        print("\nInicia la transformación de SIRAP")

        existing_siraps = self.obtain_data_from_db()
        new_siraps = self.obtain_data_from_df()
        print("Finalizada la transformación de SIRAP")

        if existing_siraps is not None and not new_siraps.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de SIRAP")

            try:

                for index, row in new_siraps.iterrows():
                    if row["normalized"] not in existing_siraps:

                        sirap = Sirap(name=row["original"])
                        self.load.add_to_session(sirap)
                        new_log.append(row["original"])
                        log_data.append(sirap)
                    else:

                        existing_log.append({"Valor":row["original"],
                                             "Error": f"Este SIRAP ya se encuentra en la base de datos"})

                if log_data:
                    
                    self.load.load_to_db(log_data, "")

                if len(existing_log) > 0:
                    self.tools.generate_csv_with_errors(existing_log, "SIRAP", "")

                msg = f'''Carga de SIRAP exitosa
                Nuevos SIRAP guardados: {len(new_log)}
                SIRAP ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los SIRAP: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)