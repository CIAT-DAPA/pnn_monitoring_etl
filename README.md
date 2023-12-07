# PNN MONITORING ETL

![GitHub release (latest by date)](https://img.shields.io/github/v/release/CIAT-DAPA/pnn_monitoring_etl) ![](https://img.shields.io/github/v/tag/CIAT-DAPA/pnn_monitoring_etl)

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
- `log/`: Folder where all errors will be saved, (it is not necessary to create it, it is created automatically).
- `import/`: This folder contains the Excel files used for data import.
- `outputs/` : Folder where all outputs will be saved, (it is not necessary to create it, it is created automatically).
- `src/`: Folder to store the source code of the project.


## Instalation

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

## config file example

The project uses a configuration file `config_file.csv` to adjust certain parameters.

- matriz_name: name of the excel sheet where the information to be imported is located.

The project uses a configuration file `db_config.csv` to adjust database connection information.

- user: This is the name of the user that will be used to access the database.
- password: This is the secret key used to authenticate and allow access to the specified user.
- host: This is the address of the server where the database is hosted.
- port: This is the port number to which the application will connect to access the database on the server.
- dbname: This is the specific name of the database to be accessed.
