import requests
from log_book import logger_config



def loginClaro(url_api,api_user,api_password):
    
    print('Ingreso Login Claro')
    logger_config.logging.info(f"Inicio Login")
    
    auth = {
        "user" : api_user,
        "password" : api_password
    }
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Transaction-ID': '19c13593-dbc3-435c-9cd0-ab073c8db505'
    }
    

    responseLogin = requests.post(url_api, headers=headers, json=auth)
    responseCode = responseLogin.json().get('statusCode')
    
    try:
        if responseLogin.status_code == 200:
            if responseLogin.json().get('type')=='JWT':
                print('Ingreso Correcto')
                logger_config.logging.info(f"Ingreso usuario y contraseña correcto")
                keyJWT = responseLogin.json().get('token')
            if responseCode == '401':
                logger_config.logging.error(f"Ingreso de Claro: Las credenciales son invalidas")
                print('El usuario y la contraseña son invalidos revisar credenciales')
        else:
            logger_config.logging.error(f"Error al ingreso de Claro: {responseLogin.status_code}")
            logger_config.logging.error(f"{responseLogin.json()}")
            print('Error al ingreso')
            print(f'{responseLogin.status_code} {responseLogin.json()}')
    except Exception as err:
        logger_config.logging.error(f'Hubo un error en el ingreso {err}')
        print(f'Hubo error en el ingreso {err}')
    
    return keyJWT


JWT=loginClaro()
