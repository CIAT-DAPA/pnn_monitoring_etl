import pandas as pd
import re
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Year

class YearT(TransformData):

    def __init__(self,data, load):
        super().__init__(data)
        self.column_year = ExcelColumns.ANNUITY.value
        self.load = load
        self.log_error_file = "year_error_log.txt"
        self.check_columns([self.column_year])

    def obtain_data_from_df(self):
            data_to_save = []
    
            try:
                print("antes for")
                for index, row in self.data["data"].iterrows():
                    print("despues for")
                    if(pd.notna(row[self.column_year]) and row[self.column_year]):
                        print("if")
                        normalize_data = self.tools.normalize_text(row[self.column_year])
                        print(normalize_data)
    
                        data = {'normalize': normalize_data, 'original': row[self.column_year]}
            
                        data_to_save.append(data)
                    
                
                df_result = pd.DataFrame(data_to_save)
    
                df_result = df_result.drop_duplicates(subset='normalize')
    
                return df_result
        
            except Exception as e:
    
                msg_error = f"Error al intentar transformar los años: {str(e)}"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
    
                return pd.DataFrame(columns=['value'])
    
    def obtain_data_from_db(self):

        try: 

            existing_year = self.load.session.query(Year.value).all()
            existing_year = set(self.tools.normalize_text(row[0]) for row in existing_year)
            return existing_year

        except Exception as e:
            msg_error = f"Error en la tabla Year al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
        
    def extract_years(value):
        # Encuentra todos los grupos de dígitos en la cadena de texto
        matches = re.findall(r'\d+', value)
        # Convierte los grupos de dígitos a enteros
        years = [int(match) for match in matches]
        return years
        
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
                    if row["normalize"] not in existing_years:

                        year = Year(value=int(row["original"]))
                        self.load.add_to_session(year)
                        new_log.append(row["original"])
                        log_data.append(year)
                    else:

                        existing_log.append(row["original"])

                if log_data:
                    
                    self.load.load_to_db(log_data)


                msg = f'''Carga de años exitosa
                Nuevos años guardados: {len(new_log)}
                Años ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los años: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)