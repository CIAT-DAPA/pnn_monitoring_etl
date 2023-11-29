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
    

    def obtain_data_from_df(self):

        data_to_save = []

        for index, row in self.data["data"].iterrows():
            if(pd.notna(row[self.column_name])):
                normalize_data = self.normalize_text(row[self.column_name])

                data = {'normalize': normalize_data, 'original': row[self.column_name]}
    
                data_to_save.append(data)
            
        
        df_result = pd.DataFrame(data_to_save)

        df_result = df_result.drop_duplicates(subset='normalize')

        return df_result

    
    def obtain_data_from_db(self):

        existing_products = self.load.session.query(Product.name).all()
        existing_products = set(self.normalize_text(row[0]) for row in existing_products)
        return existing_products
    
    def run_products(self):

        print("Inicia la transformación de productos")

        existing_products = self.obtain_data_from_db()
        new_products = self.obtain_data_from_df()

        print("Finalizada la transformación de productos")

        new_log = []
        existing_log = []

        print("Inicia la carga de productos")

        for index, row in new_products.iterrows():
            if row["normalize"] not in existing_products:

                product = Product(name=row["original"], observation='')
                self.load.add_to_session(product)
                self.load.load_to_db()
                new_log.append(row["original"])
            else:

                existing_log.append(row["original"])

        
        print("Carga de productos exitosa")
        print("Nuevos productos guardados:", len(new_log))
        print("Productos ya existentes en la base de datos:", len(existing_log))


    def normalize_text(self,data):
        text = unicodedata.normalize('NFKD', data).encode('ASCII', 'ignore').decode('utf-8')
        return text.lower().strip()