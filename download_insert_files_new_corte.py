from api import connekta_system_parameters as params
from process_modules.database_inserts import read_inserts_db_structures
from database import queries
from process_modules.ftp_downloader.download_files import download_file_ftp_siesa
from config.config import properties





def process_files(conexion_db_pyodbc,conexion_db_pythonet):
    try:
      

        print("Obteniendo lista de cortes a procesar...")
        list_cortes_to_process = queries.execute_stored_procedure("get_corte_to_process_bscs", conexion_db_pyodbc,[0])
        #SE LISTA LOS CORTES POR PROCESAR
        for corte in list_cortes_to_process:
            id_corte = corte[0]
            ciclo = corte[4]
            batch_id = corte[2]
            periodo = corte[6]
            print(f"Procesando corte ID: {id_corte}, Ciclo: {ciclo}, Batch ID: {batch_id}, Periodo: {periodo}")

            print("Obteniendo lista de archivos para descargar...")
            list_file_to_download = queries.execute_stored_procedure("sp_get_files_list_corte_bscs", conexion_db_pyodbc, [id_corte, 0])

            for file_db in list_file_to_download:
                id_file = file_db[0]
                path_local_file = file_db[3]
                path_ftp_file = file_db[4]
                id_corte = file_db[5]
          

                print(f"Descargando archivo: {path_ftp_file} a {path_local_file}")
                result_download = download_file_ftp_siesa(path_ftp_file, path_local_file)

                if result_download:
                    queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 1])
                    print(f"Archivo {path_local_file} descargado con éxito")
                else:
                    print(f"Error al descargar el archivo {path_ftp_file}")
                    queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 3])

            print("Obteniendo lista de archivos para leer e insertar en la base de datos...")

            
            file_list_to_read_insert_bd = queries.execute_stored_procedure("sp_get_files_list_corte_bscs", conexion_db_pyodbc, [id_corte, 1])
            
            if len(file_list_to_read_insert_bd)==len(list_file_to_download):

                for file_db in file_list_to_read_insert_bd:
                    id_file = file_db[0]
                    id_corte = file_db[5]
                    tipo_file = file_db[2]
                    path_local_file = file_db[3]
                   

                    print(f"Procesando archivo tipo: {tipo_file}, Archivo: {path_local_file}")

                    # Leer e insertar info de archivos en la base de datos
                    try:
                        if tipo_file == "CONTRATO":
                            read_inserts_db_structures.insert_database_contratos_file(path_local_file, id_corte, conexion_db_pythonet)
                            queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 2])

                        elif tipo_file == "Aviso_Servicios":
                            read_inserts_db_structures.insert_database_aviso_servicios_file(path_local_file, id_corte, conexion_db_pythonet)
                            queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 2])

                        elif tipo_file == "Aviso_equipo_normal":
                            read_inserts_db_structures.insert_aviso_normal_file(path_local_file, id_corte, conexion_db_pythonet)
                            queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 2])

                        elif tipo_file == "Aviso_equipo_ascard":
                            read_inserts_db_structures.insert_aviso_equipo_ascard_file(path_local_file, id_corte, conexion_db_pythonet)
                            queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 2])

                        elif tipo_file == "Retenciones_corte":
                            read_inserts_db_structures.insert_database_retenciones_file(path_local_file, periodo, id_corte, conexion_db_pythonet)
                            queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 2])

                        elif tipo_file == "BGH":
                            path_base_ftp_fe_salida=f"{properties.path_base_ftp}{properties.path_salida_fe_ftp}/{periodo}/CORTE{id_corte}_CICLO{ciclo}/"
                            path_base_local_fe_salida=f"{properties.path_base_local}/SALIDA/FE/{periodo}/CORTE{id_corte}_CICLO{ciclo}/"
                            path_base_ftp_fep_salida=f"{properties.path_base_ftp}{properties.path_salida_fep_ftp}/{periodo}/CORTE{id_corte}_CICLO{ciclo}/"
                            path_base_local_fep_salida=f"{properties.path_base_local}/SALIDA/INSUMOS/{periodo}/CORTE{id_corte}_CICLO{ciclo}/"

                           
                            read_inserts_db_structures.insert_bgh_folios_file(path_local_file, id_corte, conexion_db_pythonet, conexion_db_pyodbc, batch_id,[path_base_ftp_fe_salida,
                                                                                                                                                             path_base_local_fe_salida,
                                                                                                                                                             path_base_ftp_fep_salida,
                                                                                                                                                             path_base_local_fep_salida])
                            queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 2])
                           
                            queries.execute_update_query("sp_add_corte_info",conexion_db_pyodbc,[id_corte,properties.num_lineas_totales_corte])
                        
                        print(f"Archivo {path_local_file} procesado e insertado en la base de datos con éxito")

                    except Exception as ex:
                        print(f"Error al procesar e insertar el archivo {path_local_file}: {str(ex)}")
                        queries.execute_update_query("sp_update_file_estado_bscs", conexion_db_pyodbc, [id_file, 4])
                        raise Exception(f"Error al procesar e insertar el archivo {path_local_file}: {str(ex)}") 
                    
            else: 
                print("El numero de archivos a insertar a base de datos no corresponde con el numero de archivos descargados.\nValidar posibles errores en la descarga de archivos")    
                raise Exception(f"Error al procesar e insertar el archivo") 
            queries.execute_update_query("sp_update_corte_estado_bscs",conexion_db_pyodbc,[id_corte,1])


            

    except Exception as ex:
        print(f"Error en la ejecución del proceso: {str(ex)}")







if __name__ == "__main__":

   

    try:

         conexion_db_pyodbc=params.get_parametros_connekta("ConexionDbPyodbc")[0].get("Valor")
         conexion_db_pythonet=params.get_parametros_connekta("ConexionDbPythonet")[0].get("Valor")

        #Asignar propiedades de configuracion
         [
            properties.port_ftp_siesa,
            properties.server_ftp_siesa,
            properties.user_ftp_siesa,
            properties.path_key_file_ftp_siesa,


            properties.path_base_ftp,
            properties.path_salida_fe_ftp,
            properties.path_salida_fep_ftp,
            properties.path_base_local,
         ] = queries.execute_get_parameters_db(
            conexion_db_pyodbc,
            parameters=[
                "PuertoServidorSiesaClaroFTP",
                "ServidorSiesaClaroFTP",
                "UsuarioServidorSiesaClaroFTP",
                "RutaClavesServidorSiesaClaroFTP",



                #paths procesamiento
                "RutaBaseProcesamientoServidorSiesaClaroFTP",
                "RutaSalidaFELocal",
                "RutaSalidaInsumosLocal",
                "RutaBaseLocal"


            ])[0]
    



         process_files(conexion_db_pyodbc,conexion_db_pythonet)
         valor = input("Presiona Enter para salir")

    except Exception as ex:
        print(f"Error al inicio de ejecucion del programa: {ex}")
   




