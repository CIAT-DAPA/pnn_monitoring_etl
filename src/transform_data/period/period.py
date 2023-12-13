import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Period

class PeriodT(TransformData):

    def __init__(self, data, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date

        super().__init__(data, self.root_dir, self.actu_date)
        self.column_name = ExcelColumns.PERIOD.value
        self.log_error_file = "period_error_log.txt"

        self.check_columns([self.column_name])
    

    def obtain_data_from_df(self):

        data_to_save = []

        try:

            for index, row in self.data["data"].iterrows():
                if(pd.notna(row[self.column_name]) and row[self.column_name] 
                   and not row[self.column_name].isspace()):
                    
                    normalize_data = self.tools.normalize_text(row[self.column_name])
                    data = {'normalize': normalize_data, 'original': self.tools.clean_string(row[self.column_name])}
                    data_to_save.append(data)
            
            df_result = pd.DataFrame(data_to_save)

            df_result = df_result.drop_duplicates(subset='normalize')

            return df_result
    
        except Exception as e:

            msg_error = f"Error al intentar transformar los periodos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    
    def obtain_data_from_db(self):

        try: 

            existing_period = self.load.session.query(Period.name).all()
            existing_period = set(self.tools.normalize_text(row[0]) for row in existing_period)
            return existing_period

        except Exception as e:
            msg_error = f"Error en la tabla Period al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
    
    def run_periods(self):

        print("\nInicia la transformación de periodos")

        existing_periods = self.obtain_data_from_db()
        new_periods = self.obtain_data_from_df()

        print("Finalizada la transformación de periodos")

        if existing_periods is not None and not new_periods.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de periodos")

            try:

                for index, row in new_periods.iterrows():
                    if row["normalize"] not in existing_periods:

                        period = Period(name=row["original"])
                        self.load.add_to_session(period)
                        new_log.append(row["original"])
                        log_data.append(period)
                    else:

                        existing_log.append({"Fila": index+1, 
                                             'Valor':row["original"],
                                             "Error": f"Este periodo ya se encuentra en la base de datos"})

                if log_data:
                    
                    self.load.load_to_db(log_data, self.data["sirap_name"])

                if len(existing_log) > 0:
                    self.tools.generate_csv_with_errors(existing_log, self.column_name, self.data["sirap_name"])

                msg = f'''Carga de periodos exitosa
                Nuevos periodos guardados: {len(new_log)}
                Periodos ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los periodos: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
