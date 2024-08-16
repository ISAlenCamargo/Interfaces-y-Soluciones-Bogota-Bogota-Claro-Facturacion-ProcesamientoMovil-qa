import os
import requests

from datetime import datetime, timezone
from config.config import ApiConnekta
from dotenv import load_dotenv

def envioCantidadLineas(cantidadLineas, mensaje):
    load_dotenv()

    datosImprimir = []
    
    # Url
    baseConnekta = ApiConnekta.url_base
    servicioConnekta = ApiConnekta.servicio_log
    urlConnekta = f"{baseConnekta}{servicioConnekta}"

    idCompañia = ApiConnekta.id_compania
  
    conniKey = ApiConnekta.conni_key
    conniToken = ApiConnekta.conni_token

    idInterface = ApiConnekta.id_interfaces_base_correo
    # Hora actual
    fechaActual = datetime.now(timezone.utc)
    fechaActualstr = fechaActual.strftime('%Y-%m-%dT%H:%M:%SZ')

    paramsData = {
        "idCompania" : idCompañia,
    }
    
    headerData = {
        "conniKey" : conniKey,
        "conniToken" : conniToken
    }


        
    mensaje = f"{mensaje} cantidad de lineas insertadas: {cantidadLineas}"



    body = {
        "idinterface" : idInterface,
        "error" : False,
        "transacciones": [
            {
              "descripcion": mensaje,
              "fecha_inicio": fechaActualstr,
              "fecha_fin": fechaActualstr,
              "transaccion_exitosa": False
            }    
        ]
    }

    try:
        responseConneckta = requests.post(urlConnekta, params=paramsData, headers=headerData, json=body)
        if responseConneckta.status_code == 200:
            if responseConneckta.json().get('codigo') == 0:
                print("Reporte enviado correctamente")
        else:
            if responseConneckta.json().get('codigo') == 1:
                print(f"Error al enviar el servicio de log: {responseConneckta.json().get('detalle')}")
    except Exception as err:
        print(f"Se genero un error en el servicio Connekta Log: {err}")