import pandas as pd
import os
from tools import Tools
from pnn_monitoring_orm import Sirap
from enums import ExcelColumns

class ExtractData():


    def __init__(self, files_to_import_path, connection, config_path, actu_date):
        self.config_file_path = os.path.join(config_path, "config_file.csv")
        self.tools = Tools(os.path.join(config_path, '..'), actu_date)
        self.files_to_import_path = files_to_import_path
        self.connection = connection
        self.matriz_name = "matriz_name"
        self.log_error_file = "extract_error_log.txt"
        self.skiprows = 0
        self.dfs = []

    def read_data(self):
        # Lista para almacenar los datos y sus respectivos IDs
        data_with_ids = []

        # Obtener la lista de archivos en el path
        files = os.listdir(self.files_to_import_path)

        # Iterar sobre cada archivo
        for file in files:

            if file.endswith('.xlsx') or file.endswith('.xls'):

                file_name = os.path.splitext(file)[0]

                sheet_name = self.tools.get_specific_parameter(self.matriz_name,self.config_file_path)

                skiprows_value = self.tools.get_specific_parameter("matriz_skiprows",self.config_file_path)

                self.skiprows = int(skiprows_value) if skiprows_value != False else self.skiprows

                df = pd.read_excel(os.path.join(self.files_to_import_path, file), sheet_name=sheet_name, skiprows=self.skiprows, header=0)

                try:
                    sirap = self.connection.session.query(Sirap).filter_by(name=file_name).first()
                    if(sirap):

                        column_mapping = {
                            actual_column: enum_member.value
                            for enum_member in ExcelColumns
                            for actual_column in df.columns
                            if self.tools.normalize_text(enum_member.value) in self.tools.normalize_text(actual_column)
                            and self.tools.normalize_text(actual_column).index(self.tools.normalize_text(enum_member.value)) == 0
                        }
                        df.rename(columns=column_mapping, inplace=True)
                        data_with_ids.append({'id': sirap.id, 'data': df, 'sirap_name': sirap.name})
                        
                    else:
                        msg_error = f"No se encontro el Sirap con el ext_id: {file_name}"
                        self.tools.write_log(msg_error, self.log_error_file)
                        print(msg_error)
                except Exception as e:
                    msg_error = f"Error en la tabla Sirap: {str(e)}"
                    self.tools.write_log(msg_error, self.log_error_file)
                    print(msg_error)

        self.dfs = data_with_ids