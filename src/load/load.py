import csv
import os
from sqlalchemy.inspection import inspect
from tools import Tools

class LoadData():


    def __init__(self, session, root_dir, actu_date):
        self.root_dir = root_dir
        self.actu_date = actu_date
        self.tools = Tools(self.root_dir, self.actu_date)
        self.workspace = os.path.join(self.root_dir, "workspace")
        self.output_path = os.path.join(self.workspace, "outputs")
        format_data = self.actu_date.strftime("%Y%m%d_%H%M%S")
        self.run_file_name = f"_output.csv"
        self.log_error_file = "load_error_log.txt"
        self.session = session

        self.output_folder = os.path.join(self.output_path, format_data)
        os.makedirs(self.output_folder, exist_ok=True)

        
    def add_to_session(self, data):
        self.session.add(data)

    def load_to_db(self,data, sirap_name):

        try:

            self.session.commit()

            file_name =  sirap_name + "_" + data[0].__class__.__name__ + self.run_file_name

            mode = "a" if os.path.isfile(os.path.join(self.output_folder,file_name)) else "w"


            with open(os.path.join(self.output_folder,file_name), mode=mode, newline='') as file:
                writer = csv.writer(file)
                
                table_columns = [column.key for column in inspect(data[0]).mapper.columns]
                keys = {key: getattr(data[0], key) for key in table_columns}

                if mode == 'w':
                    writer.writerow(list(keys.keys()))

                for row in data:
                    
                    table_columns = [column.key for column in inspect(row).mapper.columns]
                    values = {key: getattr(row, key) for key in table_columns}
                    writer.writerow(list(values.values()))
        
        except Exception as e:
            msg_error = f"Error al guardar en la base de datos: {str(e)}\n"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)
            
