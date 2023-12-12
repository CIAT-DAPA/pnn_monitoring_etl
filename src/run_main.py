import os
import argparse
from etl import ETLMaster
from rollback import Rollback

def main():
    # Params
    # 0: Tipo de proceso 1: Import - 2: Rollback
    # 1.1: Ruta para importar
    # 2.1: Id del rollback

    parser = argparse.ArgumentParser(description="ETL Script")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.join(script_dir, '..')

    parser.add_argument("-prc", "--process", type=int, help="Tipo de proceso, 1: Import - 2: Rollback", required=True)

    parser.add_argument("-path", "--path", type=str, help="Ruta donde se encuentran los datos necesarios para la importación", default=parent_dir)
    parser.add_argument("-fid", "--fid", type=str, help="Id del rollback")

    args = parser.parse_args()

    if args.process == 1:

        path = args.path
        files_to_import_path = os.path.join(path,"workspace", "import")
        print(f"Iniciando importación de datos de la ruta: {files_to_import_path}\n")
        etl_master = ETLMaster(path)
        etl_master.run_etl()

    else:
        path = args.path
        if not args.fid:
            parser.error("El argumento -fid es necesario para este tipo de proceso.")
        id = args.fid
        print(f"Iniciando rollback del proceso: {id}\n")
        rollback = Rollback(path, id)
        rollback.run_rollback()


if __name__ == "__main__":
    main()

