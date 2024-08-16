from database import queries
from config.config import properties
from api import connekta_system_parameters as params
from process_modules.ftp_downloader.download_files import download_file_ftp_paradigma
from ftp_checker.ftp_checker_email_paradigma import validar_ftp_paradigma_email





if __name__ == "__main__":

    conexion_pyodbc=params.get_parametros_connekta("ConexionDbPyodbc")[0].get("Valor")
    conexion_pythonet=params.get_parametros_connekta("ConexionDbPythonet")[0].get("Valor")



    [
         properties.port_ftp_paradigma_email,
         properties.server_ftp_paradigma_email,
         properties.user_ftp_paradigma_email,
         properties.password_ftp_paradigma_email,
         properties.path_base_ftp_paradigma_email,
         properties.path_base_local

         #paths


     
     ]=queries.execute_get_parameters_db(conexion_pyodbc,
                                        
    parameters=
        ["PuertoServidorBaseCorreosParadigmaFTP",
        "ServidorBasesCorreosParadigmaFTP",
        "UsuarioServidorBaseCorreosParadigmaFTP",
        "ClaveServidorBaseCorreosParadigmaFTP",
        "RutaServidorBaseCorreosParadigmaFTP","RutaBaseLocal"])[0]

    validar_ftp_paradigma_email(conexion_pythonet,conexion_pyodbc)
    


   