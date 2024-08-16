from api import connekta_system_parameters as params
from database import queries
from config.config import properties
from process_modules.files_generation.fep_file_generation_flow import generartion_fep
import multiprocessing


def generate_fep_validate_corte(conexion_db_pyodbc):
    list_cortes_to_process_fe=queries.execute_stored_procedure("get_corte_to_process_bscs", conexion_db_pyodbc,[2])

    for corte in list_cortes_to_process_fe:
        id_corte = corte[0]
        ciclo = corte[4]
        batch_id = corte[2]

        generartion_fep(id_corte,conexion_db_pyodbc)
        queries.execute_update_query("sp_update_corte_estado_bscs",conexion_db_pyodbc,[id_corte,3])
        
        





if __name__ == "__main__":

    try:
         multiprocessing.freeze_support()
         conexion_db_pyodbc=params.get_parametros_connekta("ConexionDbPyodbc")[0].get("Valor")
      




        #Asignar propiedades de configuracion
         [
            properties.port_ftp_siesa,
            properties.server_ftp_siesa,
            properties.user_ftp_siesa,
            properties.path_key_file_ftp_siesa
         ] = queries.execute_get_parameters_db(
            conexion_db_pyodbc,
            parameters=[
                "PuertoServidorSiesaClaroFTP",
                "ServidorSiesaClaroFTP",
                "UsuarioServidorSiesaClaroFTP",
                "RutaClavesServidorSiesaClaroFTP"
            ]    )[0]  
            



         
         generate_fep_validate_corte(conexion_db_pyodbc)
  

         valor = input("Presiona Enter para salir")

    except Exception as ex:
        print(f"Error al inicio de ejecucion del programa: {ex}")