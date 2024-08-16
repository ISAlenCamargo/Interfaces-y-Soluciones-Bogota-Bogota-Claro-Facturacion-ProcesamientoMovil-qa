from api import connekta_system_parameters as params
from database import queries
from config.config import properties
from ftp_checker.ftp_checker import validate_fpt_files 




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
            properties.path_entrada_bgh_ftp,
            properties.path_base_ftp,
            properties.path_entrada_insumos_ftp,



            #Ruta base local procesamiento
            properties.path_base_local,
            properties.path_salida_fe_local,
            properties.path_ftp_insumos,
            properties.path_ftp_insumos_local,
            properties.path_salida_ftp_fe
         ] = queries.execute_get_parameters_db(
            conexion_db_pyodbc,
            parameters=[
                "PuertoServidorSiesaClaroFTP",
                "ServidorSiesaClaroFTP",
                "UsuarioServidorSiesaClaroFTP",
                "RutaClavesServidorSiesaClaroFTP",
                "RutaEntradaBghServidorClaroSiesaFtp",
                "RutaBaseProcesamientoServidorSiesaClaroFTP",
                "RutaEntradaINSUMOSServidorSiesaClaroFTP",


                #Ruta base local procesamientoo
                "RutaBaseLocal",
                "RutaSalidaFELocal",
                "RutaSalidaInsumosServidorSiesaClaroFTP",
                "RutaSalidaInsumosLocal",
                "RutaSalidaFEServidorSiesaClaroFTP"

            ]    )[0]  
            





         validate_fpt_files(conexion_db_pythonet,conexion_db_pyodbc)

         valor = input("Presiona Enter para salir")

    except Exception as ex:
        print(f"Error al inicio de ejecucion del programa: {ex}")


   






