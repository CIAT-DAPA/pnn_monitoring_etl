import pandas as pd
import os, sys
from datetime import datetime
import unicodedata
import csv

class Tools():

    def __init__(self):
        self.grandparent_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
        self.log_path = os.path.join(self.grandparent_dir, "log")
        self.output_path = os.path.join(self.grandparent_dir, "outputs")
        self.error_log_file = "log.txt"
        actu_date = datetime.now()
        format_data = actu_date.strftime("%Y%m%d_%H%M%S")
        self.csv_file_error = f"_error_{format_data}.csv"

    def get_specific_parameter(self, name, config_file_path):

        try:
            
            config = pd.read_csv(config_file_path, header=None, index_col=0).to_dict()[1]
            value = config.get(name)
            return value
        except Exception as e:
            msg_error = f'Error al obtener el valor {name} del archivo {config_file_path}: {str(e)}'
            self.write_log(msg_error, self.error_log_file)
            print(msg_error)
            sys.exit()

    def write_log(self, message, file_name, output=False):

        # Log the error message to a file

        log_path = self.output_path if output else self.log_path

        error_log_file = os.path.join(log_path, file_name)
        with open(error_log_file, 'a') as f:
            f.write(f"{datetime.now()}: {message}\n")


    def generate_csv_with_errors(self, data, tittle, sirap_name):

        try:

            file_name = f"{sirap_name}_{tittle}{self.csv_file_error}"

            mode = "a" if os.path.isfile(os.path.join(self.log_path,file_name)) else "w"


            with open(os.path.join(self.log_path,file_name), mode=mode, newline='') as file:
                writer = csv.writer(file)
                
                table_columns = data[0].keys()

                if mode == 'w':
                    writer.writerow(list(table_columns))

                for row in data:

                    values = row.values()
                    
                    writer.writerow(list(values))
        
        except Exception as e:
            msg_error = f"Error al generar el csv de errores: {str(e)}\n"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)


    def normalize_text(self,data):
        text = unicodedata.normalize('NFKD', data).encode('ASCII', 'ignore').decode('utf-8')
        return text.lower().strip()
