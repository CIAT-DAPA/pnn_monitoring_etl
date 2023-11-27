import os
import pandas as pd
from enums import ExcelColumns
from unidecode import unidecode
from tools import Tools

class TransformData():

    def __init__(self, data):
        self.data = data
        self.column_error = ""
        self.column_errors = []
        self.error_count = 0
        self.success_count = 0
        self.errors = pd.DataFrame(columns=['id', 'name', 'error'])
        self.tools = Tools()
        self.log_error_file = "transform_errors.txt"

        self.check_colums()
    

    def check_colums(self):

        expected_columns = set(enum.value for enum in ExcelColumns)
        expected_columns = {unidecode(str(column).strip().upper()) for column in expected_columns}
        actual_columns = set(self.data["data"].columns)
        actual_columns = {unidecode(str(column).strip().upper()) for column in actual_columns}


        for expected_column in expected_columns:
            check = False
            for actual_column in actual_columns:
                if expected_column in actual_column and actual_column.index(expected_column) == 0:
                    check = True
                    break
            if not check:
                self.column_errors.append(expected_column)
        
        if len(self.column_errors) > 0:
            msg_error = f"Error en las siguienets columnas: {', '.join(self.column_errors)}"
            self.tools.log_error(msg_error, self.log_error_file)
            raise ValueError(msg_error)
            

            


                    
            
        
