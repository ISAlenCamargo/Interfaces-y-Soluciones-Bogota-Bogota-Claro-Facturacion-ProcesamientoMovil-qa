from datetime import datetime
import re
import unicodedata

def format_to_2_digits(number):

    return str(number).zfill(2)
def formatearFechaGuiones(strFecha):
      if len(strFecha) != 8:
        return "Formato de fecha incorrecto. Debe ser AAAAMMDD."
      

    # Extraer el año, mes y día de la cadena de fecha
      año = strFecha[:4]
      mes = strFecha[4:6]
      dia = strFecha[6:8]

      # Formatear la fecha con guiones
      fecha_formateada = f"{año}-{mes}-{dia}"
      return fecha_formateada




def get_current_date():
    return datetime.now().strftime("%Y%m%d")


type_person_logic={
    "1":"2",
    "2":"1",
    "3":"2",
    "4":"2",
}
    
type_document_logic={
    "1":"13",
    "2":"31",
    "3":"41",
    "4":"22"
}


def contain_letters(document):
   
    return any(char.isalpha() or char == '.' for char in document)



def calculate_digito_verificacion(my_nit):
    # Se limpia el Nit
    my_nit = my_nit.replace(" ", "")  # Espacios
    my_nit = my_nit.replace(",", "")  # Comas
    my_nit = my_nit.replace(".", "")  # Puntos
    my_nit = my_nit.replace("-", "")  # Guiones

    # Se valida el nit
    if my_nit.isdigit() is False:
        print(f"El nit/cédula '{my_nit}' no es válido(a).")
        return ""

    # Procedimiento
    vpri = [3, 7, 13, 17, 19, 23, 29, 37, 41, 43, 47, 53, 59, 67, 71]
    z = len(my_nit)

    x = 0
    y = 0
    for i in range(z):
        y = int(my_nit[i])
        x += y * vpri[z - i - 1]

    y = x % 11
    
    return 11 - y if y > 1 else y



def splitFecha(strFecha):
      
      año=strFecha[:4]
      mes=strFecha[4:6]
      dia=strFecha[6:8]

      return [año,mes,dia]


def getHoraFormatoPuntos():
      hora_actual = datetime.now().strftime("%H:%M:%S")
      return hora_actual



def formatearIvaImpuestosITX(linea1120):
    valores = linea1120.split() 
    valores_con_dos_puntos = [valor for valor in valores if ':' in valor]
    valores_finales = [valor.split(':')[1] for valor in valores_con_dos_puntos]
    iva=valores_finales[0].replace(",","")
    
    consumo=valores_finales[1].replace(",","")

    return [iva,consumo]


def formatear_2Decimales(decimal):
      numero_formateado = "{:.2f}".format(decimal)
      return numero_formateado



def generar_codigo_dane(ciudad_departamento, lista_dane):
    # Convertir la entrada a minúsculas una sola vez
    ciudad_departamento = ciudad_departamento.lower()
    
    # Verificar si hay una "/" en la entrada
    if "/" in ciudad_departamento:
        ciudad, departamento = ciudad_departamento.split("/")

        ciudad=ciudad.strip()
        departamento=departamento.strip()

 
        for tupla in lista_dane:
            # Convertir los elementos de la tupla a minúsculas para la comparación
            if ciudad.lower() == tupla[2].lower() and departamento.lower() == tupla[1].lower():
               return "".join(tupla[0])
            
        for tupla in lista_dane:
             if ciudad.lower()==tupla[3].lower() and departamento.lower()==tupla[1].lower():
                  return "".join([tupla[0]])
    
    return "11001"


def remove_accents(input_str):
    # Normaliza la cadena para eliminar las tildes
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
 
def limpiezaNombre(nombre_spool):
    # Elimina las tildes del nombre
    nombre_sin_tildes = remove_accents(nombre_spool)
 
    # Elimina caracteres especiales excepto letras y números
    nombre_limpio = re.sub(r'[^a-zA-Z0-9\\s]', ' ', nombre_sin_tildes)
 
    # Elimina los espacios adicionales
    nombre_sin_espacios = " ".join(nombre_limpio.split())
 
    return nombre_sin_espacios
	
	


def limpiezaDireccion(direccionSpool):
    # Elimina caracteres especiales
    direccionLimpia = re.sub(r'[^a-zA-Z0-9\s.,#-]', ' ', direccionSpool)
   
    # Elimina espacios adicionales
    direccionSinEspacios = " ".join(direccionLimpia.split())
   
    return direccionSinEspacios



def limpiar_identificacion(identificacion):
    return re.sub(r'\D', '', identificacion)