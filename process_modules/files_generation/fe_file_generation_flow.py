import os
import pyodbc
import zipfile
from process_modules.files_generation.generation_fe import generarFe
from log_book.logger_config import logger
from process_modules.ftp_downloader.download_files import validate_or_create_directory_for_file
from connections.ftp_connection import Ftp_connection
from  multiprocessing import Manager,Process
from datetime import datetime
from config.config import properties
from database.queries import execute_update_query
from database import queries
from config.config import properties
import multiprocessing


def create_zip(result_facturas, path_local_fe,path_ftp_fe, resultados_dane, batch_id,id_corte,fecha_limite_pago,nombre_lote,id_lote,conn_str,shared_properties):
    validate_or_create_directory_for_file(path_local_fe)
    with zipfile.ZipFile(path_local_fe, 'w', zipfile.ZIP_DEFLATED) as archivo_zip:
        
        fecha_obj = datetime.strptime(fecha_limite_pago, '%d/%m/%Y')
    
        # Formatear la fecha al nuevo formato
        fecha_convertida_limite_pago = fecha_obj.strftime('%Y%m%d')

        for factura in result_facturas:
            bgh = factura[0].replace(chr(30)," ").splitlines()
            num_factura = factura[2]
            if len(factura[4]) > 0:
                email = factura[4].split(",")[3]
            else:
                email = factura[4]
            resultado = generarFe(bgh, batch_id, email, resultados_dane, id_corte,fecha_convertida_limite_pago)
         
     
            with archivo_zip.open(f"E {num_factura}.fe", 'w') as archivo_fe:
                archivo_fe.write(resultado.encode())
                
  
    del result_facturas[:]
    execute_update_query("sp_update_lote_estado_bscs",conn_str,[id_lote,1,"FE"])
    cargar_zip_ftp(path_local_fe, path_ftp_fe,nombre_lote,conn_str,id_lote,shared_properties)





def cargar_zip_ftp(ruta_local, ruta_ftp,nombre_lote,conn_str,id_lote,shared_properties):
    
   
    Conexion = Ftp_connection(int(shared_properties["port_ftp_siesa"]),shared_properties["server_ftp_siesa"],shared_properties["user_ftp_siesa"],shared_properties["path_key_file_ftp_siesa"])
    respuesta = Conexion.connect_with_keyfile()
    if respuesta:
        try:
            
            array_path_ftp=ruta_ftp.split("/")
            del array_path_ftp[-1]
            path_directory_zip="/".join(array_path_ftp)
     
            Conexion.ensure_sftp_directory_exists(path_directory_zip)
     
            Conexion.ftp.put(ruta_local, ruta_ftp)
          
            remote_file_info = Conexion.ftp.stat(ruta_ftp)
            local_file_size = os.path.getsize(ruta_local)
            remote_file_size = remote_file_info.st_size
            if local_file_size == remote_file_size:
                print(f"Prueba exitosa: El archivo para lote {nombre_lote} se cargó correctamente.")
                execute_update_query("sp_update_lote_estado_bscs",conn_str,[id_lote,2,"FE"])
            else:
                print(f"Error: Tamaño del archivo local ({local_file_size} bytes) y remoto ({remote_file_size} bytes) no coinciden para lote {nombre_lote}.")
        except Exception as e:
            print(f"An error occurred during file upload validation for lote {nombre_lote}: {e}")
        finally:
            Conexion.disconnect_sftp()





def generartion_fe(id_corte, batch_id, conn_str,fecha_limite_pago):
    try:

        manager=Manager()

        shared_properties = manager.dict()

        shared_properties["port_ftp_siesa"]=properties.port_ftp_siesa
        shared_properties["server_ftp_siesa"]=properties.server_ftp_siesa
        shared_properties["user_ftp_siesa"]=properties.user_ftp_siesa
        shared_properties["path_key_file_ftp_siesa"]=properties.path_key_file_ftp_siesa

        #validate_or_create_directory_for_file(ruta_local)
        conn = pyodbc.connect(conn_str)
        logger.info("Conexión establecida correctamente.")
        cursor = conn.cursor()
        consulta_lotes = "sp_listar_lotes_bscs " + str(id_corte)
        cursor.execute(consulta_lotes)
        resultados_lotes = cursor.fetchall()
        logger.info("Consulta de lotes ejecutada correctamente.")
        consulta_lista_dane = "sp_lista_dian_depto_municipio_bscs"
        cursor.execute(consulta_lista_dane)
        resultados_dane = cursor.fetchall()
        logger.info("Consulta de lista DANE ejecutada correctamente.")

        processes=[]
        for lote in resultados_lotes:
                consulta_factura = "sp_list_generacion_fe_bscs " + str(lote[0])
                # consulta_factura = "sp_list_generacion_fe_bscs " + str(224)
                logger.info("Iniciando consulta para lote: %s", lote[0])
                cursor.execute(consulta_factura)
                while cursor.nextset():   # NB: This always skips the first resultset
                    try:
                        resultado_facturas = cursor.fetchall()
                        break
                    except pyodbc.ProgrammingError:
                        continue
                
                logger.info("Consulta realizada para lote: %s", lote[0])
                
                
                path_local_fe=lote[2]
                path_ftp_fe=lote[3]
                nombre_lote=lote[1]
                id_lote=lote[0]

                p=Process(target=create_zip,args=(resultado_facturas,path_local_fe,path_ftp_fe,resultados_dane,batch_id,id_corte,fecha_limite_pago,nombre_lote,id_lote,conn_str,shared_properties))
                processes.append(p)
                p.start()
                

        # Esperar a que todos los procesos terminen
        for p in processes:
            p.join()
                



        logger.info("Proceso completado correctamente.")
    except pyodbc.Error as e:
        logger.error("Error de base de datos: %s", e)
        print(e)
        raise Exception(e)
    except Exception as ex:
        logger.error("Error inesperado: %s", ex)
        print(ex)
        raise Exception(ex)
        
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def generate_fe_validate_corte(conexion_db_pyodbc):

    list_cortes_to_process_fe=queries.execute_stored_procedure("get_corte_to_process_bscs", conexion_db_pyodbc,[1])
    multiprocessing.freeze_support()

    for corte in list_cortes_to_process_fe:
        id_corte = corte[0]
        ciclo = corte[4]
        batch_id = corte[2]

        properties.fecha_limite_pago = queries.execute_stored_procedure('equivalencia_get_corte_fecha_bscs', conexion_db_pyodbc, [ciclo])[0][1]

        generartion_fe(id_corte,batch_id,conexion_db_pyodbc,properties.fecha_limite_pago)
        queries.execute_update_query("sp_update_corte_estado_bscs",conexion_db_pyodbc,[id_corte,2])