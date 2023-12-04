import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Detail, Milestone, Period, Product
from datetime import datetime

class DetailT(TransformData):

    def __init__(self,data, load):
        super().__init__(data)
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
        self.load = load
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

            milestone_db = self.load.session.query(Milestone.id, Milestone.name).all()
            period_db = self.load.session.query(Period.id, Period.name).all()
            product_db = self.load.session.query(Product.id, Product.name).all()

            if milestone_db and period_db and product_db:

                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.detail_column_name]) and row[self.detail_column_name] and not row[self.detail_column_name].isspace()):

                        milestone_text = row[self.milestone_column_name] if pd.notna(row[self.milestone_column_name]) and row[self.milestone_column_name] else milestone_text
                        period_text = row[self.period_column_name] if pd.notna(row[self.period_column_name]) and row[self.period_column_name] else period_text
                        product_text = row[self.product_column_name] if pd.notna(row[self.product_column_name]) and row[self.product_column_name] else product_text
                            

                        milestone_id = self.get_match_id(milestone_text, milestone_db)
                        period_id = self.get_match_id(period_text, period_db)
                        product_id = self.get_match_id(product_text, product_db)

                        if milestone_id and period_id and product_id:

                            amount = row[self.amount_column_name] if self.validate_data(row[self.amount_column_name], 1) else False
                            quantity = row[self.quantity_column_name] if self.validate_data(row[self.quantity_column_name], 1) else False
                            goal = row[self.goal_column_name] if self.validate_data(row[self.goal_column_name], 1) else False
                            base_line = row[self.base_line_column_name] if self.validate_data(row[self.base_line_column_name], 1) else False
                            imp_value = row[self.imp_value_column_name] if self.validate_data(row[self.imp_value_column_name], 1) else False
                            

                            if (amount is not False and quantity is not False and goal is not False and 
                                base_line is not False and imp_value is not False):

                                normalize_data = self.tools.normalize_text(row[self.detail_column_name])

                                data = {'normalize': normalize_data, 'original': row[self.detail_column_name], 
                                        "amount": amount, "quantity": quantity, "goal": goal, "base_line": base_line, "imp_value": imp_value, 
                                        "milestone_id": milestone_id, "period_id": period_id, "product_id": product_id}
                                
                    
                                data_to_save.append(data)
                            
                            else:

                                data = {'original': row[self.detail_column_name], 
                                    "row": index, "column": self.detail_column_name, "error": f"Los valores del registro no corresponden a los adecuados"}

                                self.data_with_error.append(data)
                        
                        else:

                            data = {'original': row[self.detail_column_name],
                                    "row": index, "column": self.detail_column_name, "error": f"No se encontro la dependencia a la cual esta relacionado: milestone_id: {milestone_id}, period_id: {period_id}, product_id: {product_id}"}

                            self.data_with_error.append(data)
                        
                    
                
                df_result = pd.DataFrame(data_to_save)

                df_result = df_result.drop_duplicates(subset='normalize')

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

            existing_products = self.load.session.query(Detail.name, Detail.period_id, Detail.milestone_id, Detail.product_id).all()
            existing_products = set((self.tools.normalize_text(row.name), row.period_id, row.milestone_id, row.product_id) for row in existing_products)
            return existing_products

        except Exception as e:
            msg_error = f"Error en la tabla Detail al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None
    
    def run_detail(self):

        print("\nInicia la transformación de los detalles")

        existing_data = self.obtain_data_from_db()
        new_data = self.obtain_data_from_df()


        if existing_data is not None and not new_data.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de los detalles")

            try:

                existing_text_set = {text for text, _, _, _ in existing_data}
                
                

                for index, row in new_data.iterrows():
                    if (row["normalize"] not in existing_text_set 
                        or any(text == row["normalize"] and milestone_id != row["milestone_id"] for text, milestone_id, _, _ in existing_data)
                        or any(text == row["normalize"] and product_id != row["product_id"] for text, product_id, _, _ in existing_data)
                        or any(text == row["normalize"] and period_id != row["period_id"] for text, period_id, _, _ in existing_data)):

                        detail = Detail(name=row["original"], milestone_id=row["milestone_id"], product_id=row["product_id"], period_id=row["period_id"],
                                        amount=row["amount"], quantity=row["quantity"], goal=row["goal"], implemented_value=row["imp_value"],
                                          base_line=row["base_line"], date=datetime.now())
                        self.load.add_to_session(detail)
                        new_log.append(row["original"])
                        log_data.append(detail)
                    else:

                        existing_log.append(row["original"])
                    
                if log_data:
                    
                    self.load.load_to_db(log_data)
                

                msg = f'''Carga de los detalles exitosa
                Nuevos detalles guardados: {len(new_log)}
                Detalles ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los detalles: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)
        else:
            msg_error = f"No hay acciones a los cuales relacionar los detalles, por lo que no se pudo guardar los detalles\n"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)


    def validate_data(self, data, type):

        validation = False

        if type == 1 and ((data and pd.notna(data)) or data == 0):
            validation = True
        elif type == 0 and data and pd.notna(data):
            validation = True
        return validation

    def get_match_id(self, text, data_db):
        
        normalized_data_db = set((self.tools.normalize_text(row.name), row.id) for row in data_db)

        text = self.tools.normalize_text(text)
        
        matching_elements = [element for element in normalized_data_db if text in element]

        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0



