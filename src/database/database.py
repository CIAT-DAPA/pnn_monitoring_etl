from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os, sys
from pnn_monitoring_orm import Base


class PostgresConnection():

    def __init__(self, config_path):

        self.config_file = os.path.join(config_path, "config_file.csv")
        self.session = None

    def connect(self):
        try:

            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_host = os.getenv('DB_HOST')
            db_name = os.getenv('DB_NAME')

            connection_string = (
                f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}'
            )

            metadata = MetaData()

            engine = create_engine(connection_string)

            Session = sessionmaker(bind=engine)
            self.session = Session()

            Base.metadata.create_all(engine)

            print("Conexión exitosa.")
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            sys.exit()

    def disconnect(self):
        if self.session:
            self.session.close()
            print("Desconexión exitosa.")

