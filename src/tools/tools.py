import pandas as pd
import os, sys
from datetime import datetime

class Tools():

    def __init__(self):
        self.grandparent_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
        self.log_path = os.path.join(self.grandparent_dir, "log")
        self.error_log_file = "log.txt"

    def get_specific_parameter(self, name, config_file_path):

        try:
            
            config = pd.read_csv(config_file_path, header=None, index_col=0).to_dict()[1]
            value = config.get(name)
            return value
        except Exception as e:
            msg_error = f'Error al obtener el valor {name} del archivo {config_file_path}: {str(e)}'
            self.log_error(msg_error, self.error_log_file)
            print(msg_error)
            sys.exit()

    def log_error(self, message, file_name):

        # Log the error message to a file
        error_log_file = os.path.join(self.log_path, file_name)
        with open(error_log_file, 'a') as f:
            f.write(f"{datetime.now()}: {message}\n")