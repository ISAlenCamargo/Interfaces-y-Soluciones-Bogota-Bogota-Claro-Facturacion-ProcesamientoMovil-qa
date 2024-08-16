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
from process_modules.files_generation.fep_file_generation_flow import generartion_fep



def generarate_batch_id(corte):

    return f"99{format_to_four_digits(corte)}{get_current_datetime_formatted()}"

def format_to_four_digits(number):

    return str(number).zfill(4)


def get_current_datetime_formatted():

    current_datetime = datetime.now()
    return current_datetime.strftime('%Y%m%d%H%M%S')


if __name__ == "__main__":


  
        # conexion_pyodbc="DRIVER=ODBC Driver 17 for SQL Server;SERVER=CNKT-DEV-FD;DATABASE=claro_connekta_qa;UID=aycamargo;PWD=Alen12345*;fast_executemany=True;"
        # conexion_pythonnet="SERVER=CNKT-DEV-FD;DATABASE=claro_connekta_qa;User Id=aycamargo; Password=Alen12345*"
       

        conexion_pythonnet = "SERVER=10.10.23.11;DATABASE=claro_connekta_core;User Id=connekta$;Password=XSkv@dxULr4r"
        conexion_pyodbc="DRIVER=ODBC Driver 17 for SQL Server;SERVER=10.10.23.11;DATABASE=claro_connekta_core;UID=connekta$;PWD=XSkv@dxULr4r;fast_executemany=True;"
        # generartion_fep(1,"RUTA_FTP","./data/ENTRADA/","99000120240702194852",conexion_pytodbc)


        
        # read_inserts_db_structures.insert_base_email_file("./data/ENTRADA/BaseEmailMovil_20240702.txt",1,conexion_pythonnet)

        #read_inserts_db_structures.insert_bgh_folios_file("./data/ENTRADA/ALL20240701.BGH.sb.U",1,conexion_pythonnet,conexion_pytodbc,generarate_batch_id(2))

        read_inserts_db_structures.insert_bgh_folios_file("./data/ENTRADA/ALL20240701.BGH.sb.U.gz",1,conexion_pythonnet,conexion_pyodbc,generarate_batch_id(1))

        # generartion_fep(1,"RUTA_FTP","./data/SALIDA/202407/PRIVILILNG/","99000220240705194126",conexion_pyodbc)
        # read_inserts_db_structures.insert_database_retenciones_file("./data/ENTRADA/INSUMOS/202407/MES/retener.txt",202407,None,conexion_pythonnet)
        # generartion_fep(2,"RUTA_FTP","./data/SALIDA/INSUMOS/202407/CORTE01/","99000120240706111411",conexion_pyodbc)
        
        # generartion_fe(2,"","./data/SALIDA/FE/202407/CORTE02/",'99000220240706123710',conexion_pyodbc)
        #read_inserts_db_structures.insert_database_aviso_servicios_file("./data/ENTRADA/INSUMOS/202407/CORTE_01/",1,conexion_pythonnet)

        #read_inserts_db_structures.insert_database_contratos_file("./data/ENTRADA/BGH/202407/CONTRATOS20240701.dat",2,conexion_pythonnet)
        #read_inserts_db_structures.insert_aviso_equipo_ascard_file("./data/ENTRADA/202407/CORTE_02/AVISO_EQUIPOS_ASCARD_CICLO_01_CORTE_02.txt",2,conexion_pythonnet)

        # read_inserts_db_structures.insert_database_aviso_servicios_file("./data/ENTRADA/AVISO_SERVICIO_CICLO_01_CORTE_02.txt",2,conexion_pythonnet)

        # read_inserts_db_structures.insert_database_retenciones_file("")
        # generartion_fep(2,"","./data/SALIDA/INSUMOS/MUESTRAS/","",conexion_pyodbc)


# #     print(str(datetime.now()))
#     # Solicitando el parametro "cadena de conexion", Api de Connekta
#     conexion_db_pyodbc=params.get_parametros_connekta("ConexionDbPyodbc")[0].get("Valor")
#     conexion_db_pythonet=params.get_parametros_connekta("ConexionDbPythonet")[0].get("Valor")
#     parameters=params.get_parametros_connekta()

#     if len(parameters)>0:
#         if queries.truncate_table("tbl_configuraciones_bscs",conexion_db_pyodbc)==True:
#             conekta_params.insert_database_config(parameters,conexion_db_pythonet)



#     #Asignar propiedades de configuracion
#     [
#         properties.port_ftp_siesa,
#         properties.server_ftp_siesa,
#         properties.user_ftp_siesa,
#         properties.path_key_file_ftp_siesa,
#         properties.path_entrada_bgh_ftp,
#         properties.path_base_ftp,
#         properties.path_entrada_insumos_ftp,

#         #Accesos ftp paradigma
#         properties.port_ftp_paradigma_email,
#         properties.server_ftp_paradigma_email,
#         properties.user_ftp_paradigma_email,
#         properties.password_ftp_paradigma_email,
#         properties.path_base_ftp_paradigma_email,

#         #Ruta base local procesamiento
#         properties.path_base_local,
#         properties.path_salida_fe_local,
#         properties.path_ftp_insumos,
#         properties.path_ftp_insumos_local
#     ] = queries.execute_get_parameters_db(
#         conexion_db_pyodbc,
#         parameters=[
#             "PuertoServidorSiesaClaroFTP",
#             "ServidorSiesaClaroFTP",
#             "UsuarioServidorSiesaClaroFTP",
#             "RutaClavesServidorSiesaClaroFTP",
#             "RutaEntradaBghServidorClaroSiesaFtp",
#             "RutaBaseProcesamiento",
#             "RutaEntradaINSUMOS",

#             #Credenciales servidor Paradigma
#             "PuertoServidorBaseCorreosParadigmaFTP",
#             "ServidorBasesCorreosParadigmaFTP",
#             "UsuarioServidorBaseCorreosParadigmaFTP",
#             "ClaveServidorBaseCorreosParadigmaFTP",
#             "RutaServidorBaseCorreosParadigmaFTP",

#             #Ruta base local procesamientoo
#             "RutaBaseLocal",
#             "RutaSalidaFELocal",
#             "RutaSalidaInsumos",
#             "RutaSalidaInsumosLocal"
#         ]
#     )[0]

# # download_files_ftp_siesa("/sftp-claro-fd-test/ENTRADA/SIE/MOVIL/BGH/","./data/ENTRADA/BGH/202404/CORTE_15_CICLO07/")
# # download_files_ftp_siesa("/sftp-claro-fd-test/ENTRADA/SIE/MOVIL/INSUMOS/202403/MES/","./data/ENTRADA/INSUMOS/202404/MES/")
# # download_files_ftp_siesa("/sftp-claro-fd-test/ENTRADA/SIE/MOVIL/INSUMOS/202403/CORTE28_CICLO37/","./data/ENTRADA/INSUMOS/202404/CORTE_15_CICLO07/")

# # print(str(datetime.now()))
# #result_check=validate_fpt_files(conexion_db_pythonet,conexion_db_pyodbc)





#     result_check=True
#     properties.id_corte=1
#     properties.nombre_corte="ALL20240337.BGH.sb.U.gz"
#     properties.periodo="202403"
#     properties.batch_id="99003720240701122846"

#     #Generacion FE
#     path_local_generacion_fe=f"{properties.path_base_local}{properties.path_salida_fe_local}/{properties.periodo}/{properties.nombre_corte}/"
#     path_ftp_generacion_fe=f"{properties.path_base_ftp}{properties.path_salida_fe_local}/{properties.periodo}/{properties.nombre_corte}/"

#     print(path_ftp_generacion_fe)
#     generartion_fe(properties.id_corte,path_ftp_generacion_fe,path_local_generacion_fe,properties.batch_id,conexion_db_pyodbc)


# if result_check:
#     #Descargar archivos y validar ruta
#     try:
#         file_list_to_download=queries.execute_stored_procedure("sp_get_files_list_corte",conexion_db_pyodbc,[properties.id_corte,0])

#         for file_db in file_list_to_download:
#             path_local_file=file_db[3]
#             path_ftp_file=file_db[4]
#             id_corte=file_db[0]

#             result_download=download_file_ftp_siesa(path_ftp_file,path_local_file)

#             if result_download:
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,1])
#                 print("Descargado con exito")

#             else:
#                 print("Error al descargar")



#         file_list_to_read_insert_bd=queries.execute_stored_procedure("sp_get_files_list_corte",conexion_db_pyodbc,[properties.id_corte,1])
#         for file_db in file_list_to_read_insert_bd:
#             id_corte=file_db[0]
#             tipo_file=file_db[2]
#             path_local_file=file_db[3]
#             path_ftp_file=file_db[4]

#             # #leeer e insertar info de archivos a base de datos
#             if tipo_file=="CONTRATO":
#                 read_inserts_db_structures.insert_database_contratos_file(path_local_file,properties.id_corte,conexion_db_pythonet)
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,2])


#             if tipo_file=="Aviso_Servicios":
#                 read_inserts_db_structures.insert_database_aviso_servicios_file(path_local_file,properties.id_corte,conexion_db_pythonet)
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,2])
#             # if tipo_file=="Aviso_equipo_ascard":
                

#             if tipo_file=="Aviso_equipo_normal":
#                 read_inserts_db_structures.insert_aviso_normal_file(path_local_file,properties.id_corte,conexion_db_pythonet)
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,2])


#             if tipo_file=="Aviso_equipo_ascard":
#                 read_inserts_db_structures.insert_aviso_equipo_ascard_file(path_local_file,properties.id_corte,conexion_db_pythonet)
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,2])


#             if tipo_file=="Retener":
#                 read_inserts_db_structures.insert_database_retenciones_file(path_local_file,properties.periodo,properties.id_corte,conexion_db_pythonet)
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,2])

#             if tipo_file=="Retenciones_corte":
#                 read_inserts_db_structures.insert_database_retenciones_file(path_local_file,properties.periodo,properties.id_corte,conexion_db_pythonet)
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,2])


#             # #Insertar retenciones
            
#             if tipo_file=="BGH":
#                 read_inserts_db_structures.insert_bgh_folios_file(path_local_file,properties.id_corte,conexion_db_pythonet,conexion_db_pyodbc,properties.batch_id)
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,2])

#             if tipo_file=="base_email":
#                 read_inserts_db_structures.insert_base_email_file(path_local_file,properties.id_corte)
#                 queries.execute_update_query("sp_update_file_estado",conexion_db_pyodbc,[id_corte,2])


        

#         #Generacion FE
#         path_local_generacion_fe=f"{properties.path_base_local}{properties.path_salida_fe_local}/{properties.periodo}/{properties.nombre_corte}/"
#         path_ftp_generacion_fe=f"{properties.path_base_ftp}{properties.path_salida_fe_local}/{properties.periodo}/{properties.nombre_corte}/"

#         print(path_ftp_generacion_fe)
#         generartion_fe(properties.id_corte,path_ftp_generacion_fe,path_local_generacion_fe,properties.batch_id,conexion_db_pyodbc)

#        #Generacion FEP

#         path_local_generacion_fep=f"{properties.path_base_local}{properties.path_ftp_insumos_local}/{properties.periodo}/{properties.nombre_corte}/"
#         path_ftp_generacion_fep=f"{properties.path_base_ftp}{properties.path_ftp_insumos}/{properties.periodo}/{properties.nombre_corte}/"
#         print(path_ftp_generacion_fep)
#         generartion_fep(properties.id_corte,path_ftp_generacion_fep,path_local_generacion_fep,properties.batch_id,conexion_db_pyodbc)
       
       

                
#     except Exception as ex:
#         print(ex)

# else:
#     print("No se tiene corte nuevo para procesar")

# result=queries.execute_stored_procedure("sp_listar_lotes",conexion_db_pyodbc,parameters=[2])


# print(result)













