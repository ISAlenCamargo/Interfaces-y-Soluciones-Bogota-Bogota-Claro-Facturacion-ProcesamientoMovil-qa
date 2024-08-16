import time
import logging
import requests

import api
import event

def actualizacionTracking(batchID, contadorArchivos, procesador, fallos):
    # Consumo notificacion batch
    print('Generacion de Notificacion Tracking... ')
    
    #Campos
    apiBase = event.obtenerVariableConnekta("Api_URLBase_FacturaDigitalClaro_Servicios")
    apiTracking = event.obtenerVariableConnekta("Api_FacturaDigitalClaro_ActualizacionTracking")

    if procesador == 'FEP':
        evento = 'ProcesamientoConnektaFEP'
    elif procesador == 'FE':
        evento = 'ProcesamientoConnektaFE'
    else:
        print("El valor ingresado es incorrecto")

    # Url
    actualizacionUrl = f"{apiBase}{apiTracking}"

    #Log general
    logging.info(f"Ingreso a notificar Tracking")
    
    jsonData = {
        "BatchID" : batchID,
        "Event" : evento,
        "BatchSize" : contadorArchivos,
        "Failed" : fallos
    }
    
    intentos = 3

    for intento in range(intentos):
        try:
            # Notificacion 
            responseNotification = requests.put(actualizacionUrl, json=jsonData)
            logging.info(f"Intento de notificacion")

            if responseNotification.status_code == 200:
                if responseNotification.json().get('reasonPhrase') == 'OK':
                    logging.info(f"Envio exitoso al servicio notificacion de seguimiento")
                    print(responseNotification.json())
                    print('Notificacion Satisfactoria')
                    break

            else:
                logging.error(f"Ingreso a erroneo al servicio Notification rastreo: {responseNotification.status_code}")
                logging.error(f'{responseNotification.json()}')
                print(f'Notificacion Tracking Erronea: {responseNotification.status_code}')
                print(responseNotification.json())

        except ValueError as err:
            logging.info(f"Ingreso a erroneo al servicio Notification de rastreo")
            print(err)
            if intento < intentos - 1:
                time.sleep(45)
                print(f"Reintentando... ({intento + 1}/{intentos})")
            else:
                logging.error("No se pudo establecer la conexion al servicio Actualizacion Tracking despues de 3 intentos")
                print("No se pudo establecer la conexion al servicio Actualizacion Tracking después de 3 intentos")
                api.envioErrorConnekta(batchId='', tipoConexion="Servicio Actualizacion Tracking", error=err, intentos=intentos)