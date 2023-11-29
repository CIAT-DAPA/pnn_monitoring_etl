import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
import unicodedata
from models.product import Product

class ProductT(TransformData):

    def __init__(self,data, load):
        super().__init__(data)
        self.column_name = ExcelColumns.PRODUCT.value
        self.load = load
        self.log_error_file = "product_error_log.txt"
    

    def obtain_data_from_df(self):

        data_to_save = []

        try:

            for index, row in self.data["data"].iterrows():
                if(pd.notna(row[self.column_name])):
                    normalize_data = self.normalize_text(row[self.column_name])

                    data = {'normalize': normalize_data, 'original': row[self.column_name]}
        
                    data_to_save.append(data)
                
            
            df_result = pd.DataFrame(data_to_save)

            df_result = df_result.drop_duplicates(subset='normalize')

            return df_result
    
        except Exception as e:

            msg_error = f"Error al intentar transformar los productos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])

    
    def obtain_data_from_db(self):

        try: 

            existing_products = self.load.session.query(Product.name).all()
            existing_products = set(self.normalize_text(row[0]) for row in existing_products)
            return existing_products

        except Exception as e:
            msg_error = f"Error en la tabla Product al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return False
    
    def run_products(self):

        print("Inicia la transformación de productos")

        existing_products = self.obtain_data_from_db()
        new_products = self.obtain_data_from_df()

        print("Finalizada la transformación de productos")

        if existing_products and not new_products.empty:

            new_log = []
            existing_log = []

            print("Inicia la carga de productos")

            try:

                for index, row in new_products.iterrows():
                    if row["normalize"] not in existing_products:

                        product = Product(name=row["original"], observation='')
                        self.load.add_to_session(product)
                        self.load.load_to_db()
                        new_log.append(row["original"])
                    else:

                        existing_log.append(row["original"])

                msg = f'''Carga de productos exitosa
                Nuevos productos guardados: {len(new_log)}
                Productos ya existentes en la base de datos: {len(existing_log)}'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los productos: {str(e)}"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)



    def normalize_text(self,data):
        text = unicodedata.normalize('NFKD', data).encode('ASCII', 'ignore').decode('utf-8')
        return text.lower().strip()