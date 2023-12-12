import pandas as pd
import re
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Year

class YearT(TransformData):

    def __init__(self, data, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date

        super().__init__(data, self.root_dir, self.actu_date)
        self.column_year = ExcelColumns.ANNUITY.value
        self.log_error_file = "year_error_log.txt"
        self.check_columns([self.column_year])

    def obtain_data_from_df(self):
            data_to_save = []
            min_year = None
            max_year = None
    
            try:
                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.column_year]) and row[self.column_year]):
                        year=self.tools.normalize_text(str(row[self.column_year]))
                        matches = re.findall(r'\d+', year)
                        if matches:
                            matches = [int(match) for match in matches]
                            min_match = min(matches)
                            max_match = max(matches)
                            if min_year is None or min_match < min_year:
                                min_year = min_match
                            if max_year is None or max_match > max_year:
                                max_year = max_match
                for year in range(min_year, max_year + 1):
                    data = {'year': year}
                    data_to_save.append(data)
                df_result = pd.DataFrame(data_to_save)
    
                df_result = df_result.drop_duplicates(subset='year')
    
                return df_result
        
            except Exception as e:
    
                msg_error = f"Error al intentar transformar los años: {str(e)}"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
    
                return pd.DataFrame(columns=['value'])
    
    def obtain_data_from_db(self):

        try: 

            existing_year = self.load.session.query(Year.value).all()
            return existing_year

        except Exception as e:
            msg_error = f"Error en la tabla Year al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
        
        
    def run_year(self):

        print("\nInicia la transformación de años")

        existing_years = self.obtain_data_from_db()
        new_years = self.obtain_data_from_df()
        print("Finalizada la transformación de años")

        if existing_years is not None and not new_years.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de años")

            try:

                for index, row in new_years.iterrows():
                    if row["year"] not in existing_years:

                        year = Year(value=int(row["year"]))
                        self.load.add_to_session(year)
                        new_log.append(row["year"])
                        log_data.append(year)
                    else:

                        existing_log.append({"Fila": index, 
                                             'Valor':row["year"],
                                             "Error": f"Este año ya se encuentra en la base de datos"})

                if log_data:
                    
                    self.load.load_to_db(log_data, self.data["sirap_name"])

                if len(existing_log) > 0:
                    self.tools.generate_csv_with_errors(existing_log, self.column_year, self.data["sirap_name"])

                msg = f'''Carga de años exitosa
                Nuevos años guardados: {len(new_log)}
                Años ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los años: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)