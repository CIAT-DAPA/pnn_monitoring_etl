# PNN Monitoring

The main goal of this project is to design and develop a system for monitoring the activities and resources coming from different actors to finance the implementation of the Sustainability Action Plans (PAS) of the Regional Systems of Protected Areas (SIRAP). The fundamental purpose of this system is to provide authorities and stakeholders with a tool to evaluate and track the level of investment and financial support for the conservation of Colombia's protected areas.

# PNN MONITORING ETL

![GitHub release (latest by date)](https://img.shields.io/github/v/release/CIAT-DAPA/pnn_monitoring_etl) ![](https://img.shields.io/github/v/tag/CIAT-DAPA/pnn_monitoring_etl)

This component plays a key role in extracting data from external sources, such as Excel files and other sources of financial information. Its main function is to adjust, validate and transform the data so that it aligns precisely with the format and structure defined in the database [PNN Monitoring DB](https://github.com/CIAT-DAPA/pnn_monitoring_database).

**Important notes**

These Scripts must be used in conjunction with the models that was developed for the project, which you can find in this [repository](https://github.com/search?q=pnn_monitoring&type=repositories).

## Features

- Built using 
- Supports Python 3.x

## Getting Started

To use etl you must have an instance of a postgresql database.

### Prerequisites

- Python 3.x
- Postgresql
- [pnn_monitoring_orm](https://github.com/CIAT-DAPA/pnn_monitoring_orm)

### Project Structure

- `confg/`: Folder where the database credentials and other configuration data will be saved.
- `workspace/log/`: Folder where all errors will be saved, (it is not necessary to create it, it is created automatically).
- `workspace/import/`: This folder contains the Excel files used for data import.
- `workspace/outputs/` : Folder where all outputs will be saved, (it is not necessary to create it, it is created automatically).
- `src/`: Folder to store the source code of the project.


# Instalation

To use ETL we must install a set of requirements, which are in a text file, for this process we recommend to create a virtual environment, this in order not to install these requirements in the entire operating system.

1. Clone the repository
````sh
git clone https://github.com/CIAT-DAPA/pnn_monitoring_etl.git
````

2. Create a virtual environment
````sh
python -m venv env
````

3. Activate the virtual environment
- Linux
````sh
source env/bin/activate
````
- windows
````sh
env\Scripts\activate.bat
````

4. Install the required packages

````sh
pip install -r requirements.txt
````


# ETL execution

This `run_main.py` script is a key component of the PNN monitoring ETL. It allows to perform two types of processes: Data Import and Rollback.

## config file example

The project uses a configuration file `config_file.csv` to adjust certain parameters and database connection information.

- user: This is the name of the user that will be used to access the database.
- password: This is the secret key used to authenticate and allow access to the specified user.
- host: This is the address of the server where the database is hosted.
- port: This is the port number to which the application will connect to access the database on the server.
- dbname: This is the specific name of the database to be accessed.
- matriz_name: Name of the excel sheet where the information to be imported is located.
- matriz_skiprows: Number of rows to omit at the beginning of the Excel spreadsheet.

## Parameters

The script uses the following parameters:

- `-prc` or `--process`: Type of process to perform.
    - 1` for Data Import.
    - 2` for Rollback.

- `-path` or `--path`: Path where the data needed for the process is located.
    - This path must point to the folder containing the `workspace` folder with the data to be imported.

- `-fid` or `--fid`: (Required only for the Rollback process) Specific ID of the rollback to be performed.

## Usage examples

### Data import:

````bash
python run_main.py -prc 1 -path <PATH_WHERE_DATA_ARE_LOCATED>
````

Replace <PATH_WHERE_DATA_ARE_LOCATED> with the actual path where the data you want to import is located.

**Note**

During the data import a number of important files are generated:

- In the `log` folder the error files will be saved. If any error occurs or any data is not imported correctly, a folder with the format "yyyymmdd_hhmmss" representing the run will be created. Inside this folder CSV files identified with the name "sirap_column_name_error" will be generated.

- In the `outputs` folder, a folder with the same format representing the run will be created. Inside this folder, CSV files will be saved with the information of the imported data, including the IDs with which they were saved in the database. These files will have the name "sirap_table_name_output". This information will be used during the rollback process.

### Rollback:
````bash
python run_main.py -prc 2 -path <PATH_WHERE_DATA_ARE_LOCATED> -fid <ROLLBACK_ID>
````

Replace <PPATH_WHERE_DATA_ARE_LOCATED> with the path where the required data is located and <ROLLBACK_ID> with the specific ID of the rollback you want to run, is the identifier of the folder generated during the import and located in `outputs`.
