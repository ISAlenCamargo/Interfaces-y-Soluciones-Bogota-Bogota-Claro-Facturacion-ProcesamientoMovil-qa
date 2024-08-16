import logging
import requests





URL_BASE="https://clarotestfd.siesacloud.com:555/api/v1"
COMPANY_ID="800153993"
ACCOUNT_ID="0"
PROVIDER="3"

 
def notification_batch(path_batch, key_JWT, total_files, batch_ID,corte,process,array_propertys_connection_cKa):
    # Consumo notificacion batch
    print('Generacion de Notificacion Batch... ')
    
    # Url
    notificationURL = URL_BASE+ '/packages/notification'
    
    # Declaracion de campos
    packagePatch = [rutaBatch]
  
    #ToDo revisar el consumo para el campo
    corteBatch = 1
    cicloBatch = 1
    totalDocumentos = totalArchivos
    
    #Log general
    # logging.info(f"Ingreso a notificar Batch")

    headers = {
        'Authorization': f'Bearer {keyJWT}'
    }
    
    jsonData = {
        "companyId" : COMPANY_ID,
        "account" : ACCOUNT_ID,
        "batchId" : batchID,
        "packagesPaths" : packagePatch,
        "provider" : PROVIDER,
        "totalArchivos":totalArchivos,
        "pack" : corteBatch,
        "loop" : cicloBatch
        #"totalDocumentos" : totalDocumentos
    }
    
    try:
        # Notificacion 
        responseNotification = requests.post(notificationURL, json=jsonData, headers=headers)       
        # logging.info(f"Intento de notificacion")
        
        if responseNotification.status_code == 200:
            if responseNotification.json().get('companyId') == COMPANY_ID:
                # logging.info(f"Envio exitoso al servicio 'Notificacion Paquete'")
                
                #Notificacion Tracking
                #api.notificationTracking(requests, os, load_dotenv, time, batchID, contadorArchivos)
                
                print(responseNotification.json())
                print('Notificacion Satisfactoria')
                
            if responseNotification.json().get('code') == '308':
                print('JWT agotado, se intenta Login')
               #keyJWT = loginClaro()
                
                
        else:
            # logging.error(f"Ingreso erroneo al API notification Batch: {responseNotification.status_code}")
            # logging.error(f'{responseNotification.json()}')
            print('Notificacion Batch Erronea {responseNotification.status_code}')
            print(responseNotification.json())
    
    except ValueError as err:
        # logging.info(f"Ingreso a erroneo al API Notification")
        print(err)


JWT="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJTaWVzYSIsImV4cCI6IjE3MTQzOTM2NTEiLCJuYmYiOiIxNzE0MzkzNjY0IiwiaWF0IjoiMTcxNDM5MzY2NCIsIm51bWJlciI6Ik5pdCBkZSBsYSBlbXByZXNhIiwidXNlciI6InVzdWFyaW9uIG8gY29udHJhc2XDsWEgZGUgbGEgZW1wcmVzYSJ9.vFxFbEzMrfPPc-Im4_GOebvPRLmZEhaTNWVci8I_yJY"
notificationBatch("/MOVIL/FE/202404/Corte28Ciclo37/prueba01.zip",JWT,1,"99902420240311112807")
