import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Detail, Milestone, Period, Product, Time, Year, Guideline, Action
from datetime import datetime

class DetailT(TransformData):

    def __init__(self, data, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date

        super().__init__(data, self.root_dir, self.actu_date)

        self.detail_column_name = ExcelColumns.DETAIL.value
        self.amount_column_name = ExcelColumns.DETAIL_VALUE.value
        self.quantity_column_name = ExcelColumns.QUANTITY.value
        self.goal_column_name = ExcelColumns.GOAL.value
        self.annuity_column_name = ExcelColumns.ANNUITY.value
        self.period_column_name = ExcelColumns.PERIOD.value
        self.product_column_name = ExcelColumns.PRODUCT.value
        self.base_line_column_name = ExcelColumns.BASE_LINE.value
        self.milestone_column_name = ExcelColumns.MILESTONE.value
        self.imp_value_column_name = ExcelColumns.IMP_VALUE.value
        self.log_error_file = "detail_error_log.txt"
        self.data_with_error = []

        self.check_columns([self.detail_column_name, self.amount_column_name, self.quantity_column_name, self.goal_column_name,
                            self.annuity_column_name, self.period_column_name, self.product_column_name, self.base_line_column_name,
                            self.milestone_column_name, self.imp_value_column_name])
    

    def obtain_data_from_df(self):

        data_to_save = []
        milestone_text = ""
        product_text = ""
        period_text = ""

        try:

            milestone_db = self.load.session.query(
                Milestone.id,
                Milestone.name, 
                Guideline.sirap_id,
            ).join(
                Action,
                Action.id == Milestone.action_id
            ).join(
                Guideline,
                Guideline.id == Action.guideline_id
            ).all()

            period_db = self.load.session.query(Period.id, Period.name).all()
            product_db = self.load.session.query(Product.id, Product.name).all()

            if milestone_db and period_db and product_db:

                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.detail_column_name]) and row[self.detail_column_name] and not row[self.detail_column_name].isspace()):

                        milestone_text = row[self.milestone_column_name] if pd.notna(row[self.milestone_column_name]) and row[self.milestone_column_name] else milestone_text
                        period_text = row[self.period_column_name] if pd.notna(row[self.period_column_name]) and row[self.period_column_name] else period_text
                        product_text = row[self.product_column_name] if pd.notna(row[self.product_column_name]) and row[self.product_column_name] else product_text
                            

                        milestone_id = self.get_match_id(milestone_text, milestone_db, True)
                        period_id = self.get_match_id(period_text, period_db)
                        product_id = self.get_match_id(product_text, product_db)

                        if milestone_id and period_id and product_id:

                            amount = row[self.amount_column_name] if self.validate_data(row[self.amount_column_name], 1) else False
                            quantity = row[self.quantity_column_name] if self.validate_data(row[self.quantity_column_name], 1) else False
                            goal = row[self.goal_column_name] if self.validate_data(row[self.goal_column_name], 1) else False
                            base_line = row[self.base_line_column_name] if self.validate_data(row[self.base_line_column_name], 1) else False
                            imp_value = row[self.imp_value_column_name] if self.validate_data(row[self.imp_value_column_name], 1) else False

                            annuity = []

                            annuity = self.get_annuities(row[self.annuity_column_name])


                            if (amount is not False and quantity is not False and goal is not False and 
                                base_line is not False and imp_value is not False and (annuity and len(annuity) > 0)):

                                normalize_data = self.tools.normalize_text(row[self.detail_column_name])

                                data = {'normalize': normalize_data, 'original': self.tools.clean_string(row[self.detail_column_name]), 
                                        "amount": amount, "quantity": quantity, "goal": goal, "base_line": base_line, "imp_value": imp_value, 
                                        "milestone_id": milestone_id, "period_id": period_id, "product_id": product_id, "annuity": annuity}
                                
                    
                                data_to_save.append(data)
                            
                            else:

                                amount_msg = " El valor del insumo esta vacio o no corresponde al tipo de dato aceptado  " if amount is False else ""
                                quantity_msg = " La cantidad esta vacia o no corresponde al tipo de dato aceptado  " if quantity is False else ""
                                goal_msg = " La meta esta vacia o no corresponde al tipo de dato aceptado  " if goal is False else ""
                                base_line_msg = " La linea base esta vacia o no corresponde al tipo de dato aceptado  " if base_line is False else ""
                                imp_value_msg = " El valor implementado esta vacio o no corresponde al tipo de dato aceptado  " if imp_value is False else ""
                                annuity_msg = " La anualidad esta vacia o no corresponde al tipo de dato aceptado  " if not annuity or len(annuity) == 0 else ""

                                data = {"Fila": index+1, 
                                        'Valor': self.tools.clean_string(row[self.detail_column_name]),
                                        "Error": f"Los valores del registro no corresponden a los adecuados:{amount_msg}{quantity_msg}{goal_msg}{base_line_msg}{imp_value_msg}{annuity_msg}"}

                                self.data_with_error.append(data)
                        
                        else:

                            msg_mile = " No se encontro la relación con el hito " if milestone_id == 0 else ""
                            msg_pro = " No se encontro la relación con el producto " if period_id == 0 else ""
                            msg_per = " No se encontro la relación con el periodo " if period_id == 0 else ""

                            data = {"Fila": index+1,
                                    "Valor": self.tools.clean_string(row[self.detail_column_name]),
                                    "Error": f"No se encontro la dependencia a la cual esta relacionado:{msg_mile}{msg_pro}{msg_per}"}

                            self.data_with_error.append(data)
                        
                    
                
                df_result = pd.DataFrame(data_to_save)

                df_result = df_result.drop_duplicates(subset=['normalize', "milestone_id"])

                return df_result
            
            else:
                msg_error = f"No hay datos en la base de datos con los que relacionar los detalles: Milestone: {len(milestone_db)}, Period: {len(period_db)}, Product: {len(product_db)}"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)

                return pd.DataFrame(columns=['name'])
    
        except Exception as e:

            msg_error = f"Error al intentar transformar los detalles: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    
    def obtain_data_from_db(self):

        try: 

            existing_details = self.load.session.query(
                Detail.name, 
                Detail.milestone_id,
                Guideline.sirap_id
            ).join(
                Milestone,
                Milestone.id == Detail.milestone_id
            ).join(
                Action,
                Action.id == Milestone.action_id
            ).join(
                Guideline,
                Guideline.id == Action.guideline_id
            ).all()
            existing_details = set((self.tools.normalize_text(row.name), row.milestone_id, row.sirap_id) for row in existing_details)
            return existing_details

        except Exception as e:
            msg_error = f"Error en la tabla Detail al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
        

    def obtain_years_from_db(self):

        try: 

            year = self.load.session.query(Year.id, Year.value).all()
            year = set((row.id, row.value) for row in year)
            return year

        except Exception as e:
            msg_error = f"Error en la tabla Year al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
        
    
    def run_detail(self):

        print("\nInicia la transformación de los detalles")

        existing_data = self.obtain_data_from_db()
        new_data = self.obtain_data_from_df()
        years = self.obtain_years_from_db()
        ac_sirap_id = self.data["id"]


        if existing_data is not None and not new_data.empty and years is not None:

            new_log = []
            new_time_log = []
            existing_log = []
            log_data = []
            log_time = []

            print("Inicia la carga de los detalles")

            try:

                existing_text_set = {name for name, _, _ in existing_data}
                

                for index, row in new_data.iterrows():
                    if (row["normalize"] not in existing_text_set 
                        or not any(name == row["normalize"] and milestone_id == row["milestone_id"] and sirap_id == ac_sirap_id 
                                   for name, milestone_id, sirap_id in existing_data)):

                        detail = Detail(name=row["original"], milestone_id=row["milestone_id"], product_id=row["product_id"], period_id=row["period_id"],
                                        amount=row["amount"], quantity=row["quantity"], goal=row["goal"], implemented_value=row["imp_value"],
                                          base_line=row["base_line"], date=datetime.now())
                        self.load.add_to_session(detail)
                        new_log.append(row["original"])
                        log_data.append(detail)

                    else:

                        existing_log.append({"Fila": index+1, 
                                             'Valor':row["original"],
                                             "Error": f"Este registro ya se encuentra en la base de datos"})
                    
                if log_data:

                    self.load.load_to_db(log_data, self.data["sirap_name"])


                if len(existing_log) > 0 or len(self.data_with_error) > 0:

                    data_with_error = existing_log + self.data_with_error

                    self.tools.generate_csv_with_errors(data_with_error, self.detail_column_name, self.data["sirap_name"])



                print("Inicia la carga de la relación de detalles y años")
                for index, data in enumerate(log_data):
                    for year in new_data.iloc[index]["annuity"]:
                        filtered_year = [id for id, value in years if value == year][0]
                        time = Time(year_id=filtered_year, detail_id=data.id)
                        self.load.add_to_session(time)
                        new_time_log.append(year)
                        log_time.append(time)
                    
                
                if log_time:
                    
                    self.load.load_to_db(log_time, self.data["sirap_name"])

                msg = f'''Carga de los detalles exitosa
                Nuevos detalles guardados: {len(new_log)}
                Detalles ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                msg_time = f'''Carga de la relación de anualidad con los detalles exitosa
                Nuevas anualidades guardadas: {len(new_time_log)}\n'''
                print(msg_time)

                self.tools.write_log(msg, "output.txt", True)
                self.tools.write_log(msg_time, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los detalles: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
        else:
            msg_error = f"No hay hitos a los cuales relacionar los detalles, por lo que no se pudo guardar los detalles\n"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)


    def validate_data(self, data, type_of):

        validation = False

        if (isinstance(data, int) or isinstance(data, float)) and type_of == 1 and ((data and pd.notna(data)) or data == 0):
            validation = True
        elif type_of == 0 and data and pd.notna(data):
            validation = True
        return validation

    def get_match_id(self, text, data_db, milestone=False):

        matching_elements = []
        text = self.tools.normalize_text(text)
        ac_sirap_id = self.data["id"]

        if milestone:

            normalized_data_db = set((self.tools.normalize_text(row.name), row.id, row.sirap_id) for row in data_db)
            matching_elements = [element for element in normalized_data_db if text in element and ac_sirap_id == element[2]]
        else:
            normalized_data_db = set((self.tools.normalize_text(row.name), row.id) for row in data_db)
            matching_elements = [element for element in normalized_data_db if text in element]

        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0


    def get_annuities(self, data):

        range_years = "a"
        individual = ","


        if type(data) != int:
            data = data.lower().strip()
            data = data.replace(" ", "")
            data = data.replace("al", range_years)
            data = data.replace("-", individual)
            data = data.replace("y", individual)
            

        if not range_years in str(data) and not individual in str(data):
            return [data]
        
        elif range_years in str(data):
            years = data.split(range_years)
            sequence = range(int(years[0]), int(years[len(years)-1]) + 1)
            year_list = [year for year in sequence]
            return year_list

        elif individual in str(data):
            years = data.split(individual)
            years = [int(year) for year in years]
            return years
            
        