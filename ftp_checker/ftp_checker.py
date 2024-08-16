import re
from config.config import properties
from connections import ftp_connection
from log_book import logger_config
from database.queries import execute_stored_procedure
from process_modules.database_inserts.insert_file_info import insert_info_file
from process_modules.database_inserts.insert_corte import insert_corte
import os
from process_modules.data_formatting.generate_batch_id import generarate_batch_id
from stat import S_ISDIR,S_ISREG
import fnmatch
import itertools
from datetime import datetime

def format_to_two_digits(number):

    return str(number).zfill(2)


def validate_new_corte(conn_str_db_pythonet, conn_str_db_pyodbc):
    pattern_BGH = r'\bBGH\b'
    pattern_CONTRATOS = r'^CONTRATO'

    path_bgh_ftp = f"{properties.path_base_ftp}{properties.path_entrada_bgh_ftp}"
    counter_new_corte=0
    try:
        print("Iniciando conexión FTP...")
        connection = ftp_connection.Ftp_connection(int(properties.port_ftp_siesa), properties.server_ftp_siesa, properties.user_ftp_siesa, properties.path_key_file_ftp_siesa)
        request = connection.connect_with_keyfile()
        print(f"Conexión FTP establecida: {request}")
    
        if request:
            try:
                print("Listando archivos remotos...")
                remote_files = connection.ftp.listdir_attr(path_bgh_ftp)
                sorted_remote_files = group_and_sort_files(remote_files)
                

                for code, files in sorted_remote_files.items():
                    for file_attributes in files:
                        file_name = file_attributes.filename
                        file_size = file_attributes.st_size
                        _, file_extention = os.path.splitext(file_name)
                        remote_file_path = f"{path_bgh_ftp}/{file_name}"
                        estado = 0

                        local_file_path = f"{properties.path_base_local}{properties.path_entrada_bgh_ftp}/{file_name}"
                      

                        existencia_archivo = execute_stored_procedure('sp_validar_existencia_archivo_bscs', conn_str_db_pyodbc, [file_name])[0][0]
                        

                        if existencia_archivo == 0:
                            
                            if re.search(pattern_BGH, file_name):
                                print(f"Procesando archivo: {file_name}")
                                counter_new_corte+=1
                                properties.ciclo = extract_number_after_date(file_name)
                                properties.id_corte = execute_stored_procedure('equivalencia_get_corte_fecha_bscs', conn_str_db_pyodbc, [properties.ciclo])[0][0]
                                properties.batch_id = generarate_batch_id(properties.id_corte)
                                properties.periodo = extract_year_month(file_name)
                                properties.nombre_corte = file_name
                                print(f"Archivo BGH detectado: {file_name}, Ciclo: {properties.ciclo}, ID Corte: {properties.id_corte}")

                                existencia_corte = execute_stored_procedure('sp_validar_existencia_corte_bscs', conn_str_db_pyodbc, [file_name])[0][0]
                                print(f"Existencia del corte {file_name}: {existencia_corte}")

                                if existencia_corte == 0:
                                    print(f"Insertando corte y archivo BGH: {file_name}")
                                    insert_corte(conn_str_db_pythonet, file_name, properties.batch_id, properties.id_corte, properties.ciclo, properties.periodo)
                                    insert_info_file(conn_str_db_pythonet, file_name, "BGH", file_extention, file_size, local_file_path, remote_file_path, estado, properties.id_corte, properties.periodo)

                            if properties.id_corte is not None and re.search(pattern_CONTRATOS, file_name):
                                print(f"Archivo CONTRATO detectado: {file_name}")
                                insert_info_file(conn_str_db_pythonet, file_name, "CONTRATO", file_extention, file_size, local_file_path, remote_file_path, estado, properties.id_corte, properties.periodo)

                    if properties.id_corte is not None and existencia_archivo==0: validate_new_ftp_insumos(conn_str_db_pythonet, conn_str_db_pyodbc)
                if counter_new_corte==0:
                    print()
                    print("NO HUBO ARCHIVOS NUEVOS PARA PROCESAR")
                    print()


            except Exception as ex:
                print(f"Error al procesar archivos remotos: {str(ex)}")
                return False

    except Exception as ex:
        print(f"Error al conectar FTP: {str(ex)}")
        return False





def validate_new_ftp_insumos(conn_str_db_pythonet, conn_str_db_pyodbc):
    try:
        pattern_retenciones_corte = r'(?i)retenciones'
        print("Iniciando conexión FTP para insumos...")
        connection = ftp_connection.Ftp_connection(int(properties.port_ftp_siesa), properties.server_ftp_siesa, properties.user_ftp_siesa, properties.path_key_file_ftp_siesa)
        request = connection.connect_with_keyfile()
        print(f"Conexión FTP establecida para insumos: {request}")

        if request:
            path_ftp_insumos = f"{properties.path_base_ftp}{properties.path_entrada_insumos_ftp}/{properties.periodo}"
            print(f"Ruta FTP de insumos: {path_ftp_insumos}")

            remote_files_insumos = connection.ftp.listdir_attr(path_ftp_insumos)
           

            pattern = f'*_corte_{format_to_two_digits(properties.id_corte)}*'.lower()
            remote_filtered_files = [file for file in remote_files_insumos if fnmatch.fnmatch(file.filename.lower(), pattern)]

            for insumo in remote_filtered_files:
                if S_ISREG(insumo.st_mode):
                    file_name = insumo.filename
                    file_size = insumo.st_size
                    _, file_extention = os.path.splitext(file_name)
                    remote_file_path = f"{path_ftp_insumos}/{file_name}"
                    estado = 0
                    path_local_insumo = f"{properties.path_base_local}{properties.path_entrada_insumos_ftp}/{properties.periodo}/CORTE{properties.id_corte}/{file_name}"
                    print(f"Procesando archivo de insumo: {file_name}")

                    existencia_archivo = execute_stored_procedure('sp_validar_existencia_archivo_bscs', conn_str_db_pyodbc, [file_name])[0][0]
                    print(f"Existencia del archivo {file_name}: {existencia_archivo}")

                    if existencia_archivo == 0:
                        if validar_nombre_aviso_equipo(file_name, "AVISO_EQUIPO"):
                            print(f"Insertando archivo Aviso_equipo_normal: {file_name}")
                            insert_info_file(conn_str_db_pythonet, file_name, "Aviso_equipo_normal", file_extention, file_size, path_local_insumo, remote_file_path, estado, properties.id_corte, properties.periodo)

                        if validar_nombre_aviso_equipo(file_name, "AVISO_EQUIPOS_ASCARD"):
                            print(f"Insertando archivo Aviso_equipo_ascard: {file_name}")
                            insert_info_file(conn_str_db_pythonet, file_name, "Aviso_equipo_ascard", file_extention, file_size, path_local_insumo, remote_file_path, estado, properties.id_corte, properties.periodo)

                        if validar_nombre_aviso_equipo(file_name, "AVISO_SERVICIO"):
                            print(f"Insertando archivo Aviso_Servicios: {file_name}")
                            insert_info_file(conn_str_db_pythonet, file_name, "Aviso_Servicios", file_extention, file_size, path_local_insumo, remote_file_path, estado, properties.id_corte, properties.periodo)

                        if re.search(pattern_retenciones_corte, file_name):
                            print(f"Insertando archivo Retenciones_corte: {file_name}")
                            insert_info_file(conn_str_db_pythonet, file_name, "Retenciones_corte", file_extention, file_size, path_local_insumo, remote_file_path, estado, properties.id_corte, properties.periodo)

    except Exception as ex:
        print(f"Error al procesar archivos de insumos: {str(ex)}")
        print(f"Archivo problemático: {file_name if 'file_name' in locals() else 'Desconocido'}")




def validate_fpt_files(conn_str_db_pythonet,conn_str_db_pyodbc):

   try:
       
        validate_new_corte(conn_str_db_pythonet,conn_str_db_pyodbc)
        



   except Exception as e:
    print(e)







def extract_number_after_date(filename):
    
    # Expresión regular para encontrar el patrón ALLYYYYMM(\d+)
    pattern = r'ALL\d{6}(\d{2})'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    else:
        return None
    

def extract_year_month(filename):
   
    # Expresión regular para encontrar el patrón ALLYYYYMM
    pattern = r'ALL(\d{6})\d{2}'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    else:
        return None
    

def validar_nombre_aviso_equipo(nombre_archivo, cadena_a_buscar):
    """
    Función para validar que una cadena específica esté presente únicamente al inicio del nombre del archivo.

    Args:
    - nombre_archivo (str): El nombre del archivo a verificar.
    - cadena_a_buscar (str): La cadena exacta que se desea buscar al inicio del nombre del archivo.

    Returns:
    - bool: True si la cadena está presente al inicio del nombre del archivo y no seguida por caracteres alfanuméricos, False de lo contrario.
    """
    if nombre_archivo.startswith(cadena_a_buscar) and \
       (len(nombre_archivo) == len(cadena_a_buscar) or not nombre_archivo[len(cadena_a_buscar)].isalnum()):
        return True
    else:
        return False
    



def group_and_sort_files(remote_files):
    # Función para extraer el código numérico del nombre del archivo
    def extract_code(filename):
        match = re.search(r'\d+', filename)
        return match.group(0) if match else ''

    # Función para obtener la fecha del archivo desde la propiedad 'st_atime'
    def extract_date(file):
        # Suponiendo que 'st_atime' es un timestamp en segundos desde epoch
        # Convierte 'st_atime' a un objeto datetime
        return datetime.fromtimestamp(file.st_atime) if hasattr(file, 'st_atime') else datetime.min

    # Función para dar prioridad a los archivos que contienen "BGH"
    def sort_key(file):
        filename = file.filename  # o usa file si es una cadena
        return (extract_code(filename), 0 if 'BGH' in filename else 1, filename)

    # Ordenar los archivos por clave de prioridad
    sorted_files = sorted(remote_files, key=sort_key)

    # Agrupar los archivos por código
    grouped_files = {key: list(group) for key, group in itertools.groupby(sorted_files, key=lambda file: extract_code(file.filename))}

    # Ordenar los códigos por la fecha más reciente de los archivos en cada grupo
    sorted_codes = sorted(
        grouped_files.keys(),
        key=lambda code: max(extract_date(file) for file in grouped_files[code]),
        reverse=True
    )[:5]

    # Filtrar los grupos para obtener solo los últimos 5 basados en la fecha más reciente
    filtered_grouped_files = {code: grouped_files[code] for code in sorted_codes}

    return filtered_grouped_files







# def group_and_sort_files(remote_files):
#     # Función para extraer el código numérico del nombre del archivo
#     def extract_code(filename):
#         match = re.search(r'\d+', filename)
#         return match.group(0) if match else ''

#     # Función para dar prioridad a los archivos que contienen "BGH"
#     def sort_key(file):
#         filename = file.filename  # o usa file si es una cadena
#         return (extract_code(filename), 0 if 'BGH' in filename else 1, filename)

#     # Ordenar los archivos
#     sorted_files = sorted(remote_files, key=sort_key)

#     # Agrupar los archivos por código
#     grouped_files = {key: list(group) for key, group in itertools.groupby(sorted_files, key=lambda file: extract_code(file.filename))}

#     return grouped_files