import json
import time
import requests
from config.config import ApiConnekta
from process_modules.database_inserts import insert_params_config as database
from log_book import logger_config



def config_mapping(parameters, filter_value, key_name='Nombre'):
    for config in parameters:
        if config[key_name] == filter_value:
            return config['Valor']


def get_parametros_connekta(filtro_parametro=""):

    # Consumo notificacion Connekta
    print('Obteniendo parametros de Connekta...')
    

    paramsData = {
        "idCompania"               : ApiConnekta.id_compania,
        "idSistema"                : ApiConnekta.id_sistema,
        "descripcionParametro"     :filtro_parametro
    }
    
    headerData = {
        "conniKey" : ApiConnekta.conni_key,
        "conniToken" : ApiConnekta.conni_token
    }
    
    intentos = 3
    url_connekta=f"{ApiConnekta.url_base}{ApiConnekta.url_parametros_sistema}"

    for intento in range(intentos):
        try:
            # Valores 
            responseConneckta = requests.get(url_connekta, params=paramsData, headers=headerData)
            if responseConneckta.status_code == 200:
                if responseConneckta.json().get('codigo') == 0:

                    detalles = responseConneckta.json().get('detalle')

                    parametros = detalles.get('Parametros')

                    #Envio Insercion
                    return parametros
                    #break

            else:
                if responseConneckta.json().get('codigo') == 1:
                    print(f"Error al traer las variables: {responseConneckta.json().get('detalle')}")

        except Exception as err:
            logger_config.logging.info(f"Ingreso a erroneo al servicio Connekta {err}")
            if intento < intentos - 1:
                time.sleep(45)
                print(f"Reintentando... ({intento + 1}/{intentos})")
            else:
                logger_config.logging.error("No se pudo establecer la conexion al servicio Variables Connekta despues de 3 intentos")
                print("No se pudo establecer la conexion al servicio Variables Connekta después de 3 intentos")
                #api.envioCorreoConnekta(batchId='', tipoConexion="Servicio Variables Connekta", error=err, intentos=intentos)