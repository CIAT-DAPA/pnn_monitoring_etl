from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
from pnn_monitoring_orm import Base


class PostgresConnection():

    def __init__(self, config_path):

        self.config_file = os.path.join(config_path, "db_config.csv")
        self.config = pd.read_csv(self.config_file, header=None, index_col=0).to_dict()[1]
        self.session = None

    def connect(self):
        try:

            connection_string = (
                f'postgresql://{self.config["user"]}:{self.config["password"]}@{self.config["host"]}/{self.config["dbname"]}'
            )

            metadata = MetaData()

            engine = create_engine(connection_string)

            Session = sessionmaker(bind=engine)
            self.session = Session()

            Base.metadata.create_all(engine)

            print("Conexión exitosa.")
        except Exception as e:
            print(f"Error al conectar: {e}")

    def disconnect(self):
        if self.session:
            self.session.close()
            print("Desconexión exitosa.")

