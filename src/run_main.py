from etl import ETLMaster
import os
import argparse


def main():
    # Params
    # 0: Tipo de proceso 1: Import - 2: Rollback
    # 1.1: Ruta para importar
    # 2.1: Id del rollback

    parser = argparse.ArgumentParser(description="ETL Script")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.join(script_dir, '..')
    files_to_import_path = os.path.join(parent_dir, "import")

    parser.add_argument("-prc", "--process", type=int, help="Type of process", required=True)

    parser.add_argument("-path", "--path", type=str, help="Path to import")
    parser.add_argument("-fid", "--fid", type=str, help="Folder Id")

    args = parser.parse_args()

    if args.process == 1:

        files_to_import_path = os.path.join(parent_dir, "import")
        path = args.path or files_to_import_path
        print(f"Iniciando importación de datos de la ruta: {path}\n")
        etl_master = ETLMaster(parent_dir)
        etl_master.run_etl()

    else:

        if not args.fid:
            parser.error("El argumento -fid es necesario para este tipo de proceso.")
        id = args.fid
        print(f"Iniciando rollback del proceso: {id}\n")



if __name__ == "__main__":
    main()

