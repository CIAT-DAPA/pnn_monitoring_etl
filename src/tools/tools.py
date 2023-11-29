import pandas as pd
import os, sys
from datetime import datetime

class Tools():

    def __init__(self):
        self.grandparent_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
        self.log_path = os.path.join(self.grandparent_dir, "log")
        self.output_path = os.path.join(self.grandparent_dir, "outputs")
        self.error_log_file = "log.txt"

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
