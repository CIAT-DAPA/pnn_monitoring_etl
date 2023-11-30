from datetime import datetime
import csv
import os
from sqlalchemy.inspection import inspect

class LoadData():


    def __init__(self, session):
        self.grandparent_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
        self.output_path = os.path.join(self.grandparent_dir, "outputs")
        actu_date = datetime.now()
        format_data = actu_date.strftime("%Y%m%d_%H%M%S")
        self.run_file_name = f"_output_{format_data}.csv"
        self.session = session

        
    def add_to_session(self, data):
        self.session.add(data)

    def load_to_db(self,data):
        self.session.commit()

        file_name =  data[0].__class__.__name__ + self.run_file_name

        mode = "a" if os.path.isfile(os.path.join(self.output_path,file_name)) else "w"


        with open(os.path.join(self.output_path,file_name), mode=mode, newline='') as file:
            writer = csv.writer(file)
            
            table_columns = [column.key for column in inspect(data[0]).mapper.columns]
            keys = {key: getattr(data[0], key) for key in table_columns}

            if mode == 'w':
                writer.writerow(['Module'] + list(keys.keys()))

            for row in data:
                
                table_columns = [column.key for column in inspect(row).mapper.columns]
                values = {key: getattr(row, key) for key in table_columns}
                writer.writerow([row.__class__.__name__] + list(values.values()))
            
