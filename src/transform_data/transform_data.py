from enums import ExcelColumns
from unidecode import unidecode
from tools import Tools

class TransformData():

    def __init__(self, data, root_dir, actu_date):
        self.data = data
        self.column_error = ""
        self.column_errors = []
        self.tools = Tools(root_dir, actu_date)
        self.log_error_file = "transform_errors.txt"

        self.check_columns()
    

    def check_columns(self, expected_columns=None):
        
        if expected_columns is None:
            expected_columns = set(enum.value for enum in ExcelColumns)
            expected_columns = {unidecode(str(column).strip().upper()) for column in expected_columns}
        else:
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
            msg_error = f"\nError en las siguienets columnas: {', '.join(self.column_errors)}"
            self.tools.write_log(msg_error, self.log_error_file)
            raise ValueError(msg_error)
            

            


                    
            
        
