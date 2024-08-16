import os
from process_modules.ftp_downloader import download_files
from api import connekta_system_parameters as params
from process_modules.database_inserts import insert_params_config as conekta_params,read_inserts_db_structures
from database import queries
from datetime import datetime
from config.config import properties
from ftp_checker.ftp_checker import validate_fpt_files 
from process_modules.ftp_downloader.download_files import download_file_ftp_siesa
from process_modules.files_generation.fe_file_generation_flow import generartion_fe
from process_modules.files_generation.fep_file_generation_flow import generartion_fep,prueba_bgh





conexion_db_pyodbc=params.get_parametros_connekta("ConexionDbPyodbc")[0].get("Valor")
conexion_db_pythonet=params.get_parametros_connekta("ConexionDbPythonet")[0].get("Valor")
parameters=params.get_parametros_connekta()

if len(parameters)>0:
        if queries.truncate_table("tbl_configuraciones_bscs",conexion_db_pyodbc)==True:
            conekta_params.insert_database_config(parameters,conexion_db_pythonet)