import re
import os
from config.config import properties
from connections import ftp_connection
from database.queries import execute_stored_procedure
from process_modules.database_inserts.insert_file_info import insert_info_file
from process_modules.database_inserts.read_inserts_db_structures import insert_base_email_file
from process_modules.ftp_downloader.download_files import download_file_ftp_paradigma
from datetime import datetime
from api.connekta_logs import envioCantidadLineas

def validar_ftp_paradigma_email(conn_str_db_pythonet,conn_str_db_pyodbc):
    try:
                        

                        connection = ftp_connection.Ftp_connection(int(properties.port_ftp_paradigma_email), properties.server_ftp_paradigma_email, properties.user_ftp_paradigma_email, properties.password_ftp_paradigma_email)
                        request = connection.connect()

                        if request:

                            remote_file_paradigma=connection.ftp.listdir_attr(properties.path_base_ftp_paradigma_email)


                            for file_email in  remote_file_paradigma:
                                file_name = file_email.filename
                                file_size = file_email.st_size
                                _,file_extention=os.path.splitext(file_name)
                                remote_file_path=f"{properties.path_base_ftp_paradigma_email}{file_name}"
                                local_file_path=f"{properties.path_base_local}{properties.path_base_ftp_paradigma_email}{file_name}"
                                estado=0

                                existencia_archivo=execute_stored_procedure('sp_validar_existencia_archivo_bscs',conn_str_db_pyodbc,[file_name])[0][0]

                                if existencia_archivo==0:
                                    insert_info_file(conn_str_db_pythonet,file_name,"base_email",file_extention,file_size,local_file_path,remote_file_path,estado,None,None)
                                    print("Inicio de descarga: ",datetime.now())
                                    download_file_ftp_paradigma(remote_file_path,local_file_path)
                                    print("Fin de descarga: ",datetime.now())
                                    print("Inicio para insertar en BBDD: ",datetime.now())
                                    insert_base_email_file(local_file_path,0,conn_str_db_pythonet,conn_str_db_pyodbc)
                                    envioCantidadLineas(3,f"Base de correos electronicos fue procesada correctamente, nombre del archivo procesado: {file_name}")
                                    print("Fin para insertar en BBDD: ",datetime.now())

                            
                            if len(remote_file_paradigma)==0:
                                print("No hay base email")


    except Exception as ex:
         print(str(ex))