import event
import requests
import logging

from datetime import datetime
from dotenv import load_dotenv

def convertidorFecha(fechaOriginal):

     # Reemplazar "a.m." y "p.m." por "AM" y "PM" que son reconocidos por strptime
     fecha_hora_original = fechaOriginal.replace("a.m.", "AM").replace("p.m.", "PM")

     # Convierte la fecha y hora
     fecha_hora_objeto = datetime.strptime(fecha_hora_original, "%d/%m/%Y %I:%M:%S %p")
     fecha_hora_formateada = fecha_hora_objeto.strftime("%Y-%m-%d %H:%M:%S")

     return fecha_hora_formateada

def obtencionResolucion():
     load_dotenv()
     # Consumo notificacion Connekta
     print('Obteniendo valores de la resolucion...')
    
     # Url
     urlBase = event.obtenerVariableConnekta("Api_URLBase_FacturaDigitalClaro_Servicios")
     urlPackage = event.obtenerVariableConnekta("Api_ResolucionClaro")
     urlApi = f"{urlBase}{urlPackage}"

     companyId = "800153993"
    
     body = {
          "companyId" : companyId
     }
    
     try:
          # Valores 
          responseResolucion = requests.post(urlApi, json=body)
          if responseResolucion.status_code == 200:
               if responseResolucion.json().get('codigo') == "1":
                    print('Obtencion correcta')

                    resultado = responseResolucion.json().get("resoluciones")
                    for contador in range(len(resultado)):
                         if resultado[contador].get("PREFIJO") == "R":
                              numeroResolucion = resultado[contador].get("NUMERO_RESOLUCION")
                              fechaResolucion = resultado[contador].get("FECHA_RESOLUCION")
                              rangoInicial = resultado[contador].get("RANGO_INICIAL")
                              rangoFinal = resultado[contador].get("RANGO_FINAL")
                              vigenciaDesde = resultado[contador].get("VIGENCIA_DESDE")
                              vigenciaHasta = resultado[contador].get("VIGENCIA_HASTA")
                              claveTecnica = resultado[contador].get("CLAVE_TECNICA")
                              nitFacturador = resultado[contador].get("NIT_FACTURADOR")
                              prefijoFacturador = resultado[contador].get("PREFIJO")
                              ambiente = "1"

                              fechaResolucionFormateada = convertidorFecha(fechaResolucion)
                              vigenciaDesdeFormateada = convertidorFecha(vigenciaDesde)
                              vigenciaHastaFormateada = convertidorFecha(vigenciaHasta)

                              event.insertarValoresResolucion(numeroResolucion, fechaResolucionFormateada, rangoInicial, rangoFinal,
                                                              vigenciaDesdeFormateada, vigenciaHastaFormateada, claveTecnica, nitFacturador,
                                                              prefijoFacturador, ambiente)
                              
                         if resultado[contador].get("PREFIJO") == "SETT":
                              numeroResolucion = resultado[contador].get("NUMERO_RESOLUCION")
                              fechaResolucion = resultado[contador].get("FECHA_RESOLUCION")
                              rangoInicial = resultado[contador].get("RANGO_INICIAL")
                              rangoFinal = resultado[contador].get("RANGO_FINAL")
                              vigenciaDesde = resultado[contador].get("VIGENCIA_DESDE")
                              vigenciaHasta = resultado[contador].get("VIGENCIA_HASTA")
                              claveTecnica = resultado[contador].get("CLAVE_TECNICA")
                              nitFacturador = resultado[contador].get("NIT_FACTURADOR")
                              prefijoFacturador = resultado[contador].get("PREFIJO")
                              ambiente = "0"

                              fechaResolucionFormateada = convertidorFecha(fechaResolucion)
                              vigenciaDesdeFormateada = convertidorFecha(vigenciaDesde)
                              vigenciaHastaFormateada = convertidorFecha(vigenciaHasta)

                              event.insertarValoresResolucion(numeroResolucion, fechaResolucionFormateada, rangoInicial, rangoFinal,
                                                              vigenciaDesdeFormateada, vigenciaHastaFormateada, claveTecnica, nitFacturador,
                                                              prefijoFacturador, ambiente)

               else:
                    if responseResolucion.json().get('codigo') == "-1":
                         print(f"Error al traer las resoluciones: {responseResolucion.json().get('mensaje')}")
                         logging.error(f"Error al traer las resoluciones: {responseResolucion.json().get('mensaje')}")
          else:
               logging.error("Fallo al obtener los valores de la resolucion")

     except Exception as err:
          logging.info(f"Ingreso a erroneo al servicio de resoluciones: {err}")
