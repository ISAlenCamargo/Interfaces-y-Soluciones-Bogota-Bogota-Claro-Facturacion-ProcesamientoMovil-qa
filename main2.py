from process_modules.ftp_downloader import download_files
from api.connekta_system_parameters import get_parametros_connekta,config_mapping
from process_modules.database_inserts import insert_params_config as conekta_params,read_inserts_db_structures
from database import queries
from datetime import datetime
from config.config import properties
from ftp_checker.ftp_checker import validate_fpt_files 
from process_modules.ftp_downloader.download_files import download_file_ftp_siesa
from process_modules.files_generation.fe_file_generation_flow import generate_fe_validate_corte
from process_modules.files_generation.fep_file_generation_flow import generartion_fep,prueba_bgh
from process_modules.ftp_downloader.download_insert_files_new_corte import process_files
import multiprocessing



if __name__ == "__main__":


    interface=203
    try:
        #Carga de propiedades de Connekta
        parameters=get_parametros_connekta()

        properties.conexion_bd_pyodbc = config_mapping(parameters,"ConexionDbPyodbc")
        properties.conexion_db_pythonet = config_mapping(parameters,"ConexionDbPythonet")
        properties.port_ftp_siesa = config_mapping(parameters,"PuertoServidorSiesaClaroFTP")
        properties.server_ftp_siesa = config_mapping(parameters,"ServidorSiesaClaroFTP")
        properties.user_ftp_siesa = config_mapping(parameters,"UsuarioServidorSiesaClaroFTP")
        properties.path_key_file_ftp_siesa = config_mapping(parameters,"RutaClavesServidorSiesaClaroFTP") 
        properties.path_entrada_bgh_ftp = config_mapping(parameters,"RutaEntradaBghServidorClaroSiesaFtp")
        properties.path_base_ftp = config_mapping(parameters,"RutaBaseProcesamientoServidorSiesaClaroFTP")
        properties.path_entrada_insumos_ftp = config_mapping(parameters,"RutaEntradaINSUMOSServidorSiesaClaroFTP")
        properties.path_base_local = config_mapping(parameters,"RutaBaseLocal")
        properties.path_salida_fe_local = config_mapping(parameters,"RutaSalidaFELocal")
        properties.path_ftp_insumos = config_mapping(parameters,"RutaSalidaInsumosServidorSiesaClaroFTP")
        properties.path_ftp_insumos_local = config_mapping(parameters,"RutaSalidaInsumosLocal")
        properties.path_salida_ftp_fe = config_mapping(parameters,"RutaSalidaFEServidorSiesaClaroFTP")
        #ApiTimbradoMasivo
        properties.url_base_api_timbrado = config_mapping(parameters,"ApiTimbradoMasivoUrlBase")
        properties.url_login_api_timbrado = config_mapping(parameters,"ApiTimbradoMasivoUrlLogin")
        properties.user_api_timbrado = config_mapping(parameters,"ApiTibradoMasivoUser")
        properties.password_api_timbrado = config_mapping(parameters,"ApiTibradoMasivoPassword")
        properties.XTransacctionId_api_timbrado = config_mapping(parameters,"ApiTimbradoMasivoXTransacctionID")
        properties.companyId_api_timbrado = config_mapping(parameters,"ApiTimbradoMasivoCompanyId")
        properties.count_api_timbrado = config_mapping(parameters,"ApiTimbradoMasivoAccount")
        properties.provider_api_timbrado = config_mapping(parameters,"ApiTimbradoMasivoProvider")
        properties.previlling_api_timbrado = bool(config_mapping(parameters,"ApiTimbradoMasivoPrevinlling"))
        properties.type_api_timbrado = config_mapping(parameters,"ApiTimbradoMasivoType")

        if interface==201:

            #Chequear existencia de nuevo corte
            validate_fpt_files(properties.conexion_db_pythonet,properties.conexion_bd_pyodbc)


        elif interface==202:
            #Validar corte en base de datos por descargar ARCHIVOS e INSERTAR a base de datos la informacion de estos
            process_files(properties.conexion_bd_pyodbc,properties.conexion_db_pythonet)


        elif interface==203:
             #Generacion de archivos FE 
             generate_fe_validate_corte(properties.conexion_bd_pyodbc)

 




         

  

       

    except Exception as ex:
        print(f"Error al inicio de ejecucion del programa: {ex}")


