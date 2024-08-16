import os
import pyodbc
import zipfile
from  multiprocessing import Manager,Process
from process_modules.files_generation.generation_fe import generarFe
from log_book.logger_config import logger
from config.config import properties
from process_modules.ftp_downloader.download_files import validate_or_create_directory_for_file
from connections.ftp_connection import Ftp_connection
import time
from database.queries import execute_update_query



def generarFep(factura, num_factura_prueba):
    fila_1 = 0
    resultado = ""

    for elemento in factura:
        if fila_1 == 0:
            fila_1 += 1
            continue
        if fila_1 == 1:
            fila_1 += 1
            if elemento.strip():
                resultado += elemento
                continue
        if elemento.strip():
            resultado += elemento + "\n"

    resultado += f"DFAC|E|{str(num_factura_prueba)}"
    return resultado




def crear_zip(resultado_facturas, path_local_fep, path_ftp_fep,id_lote,shared_properties,conn_str):
    validate_or_create_directory_for_file(path_local_fep)
    with zipfile.ZipFile(path_local_fep, 'w', zipfile.ZIP_DEFLATED) as archivo_zip:
        
        for factura in resultado_facturas:
            num_factura = factura[0]
            resultado = generarFep(factura, num_factura)
            
            with archivo_zip.open(f"E {num_factura}.fep", 'w') as archivo_fe:
                archivo_fe.write(resultado.encode())
                
    print(".zip creado")
    del resultado_facturas[:]
    execute_update_query("sp_update_lote_estado_bscs",conn_str,[id_lote,1,"FEP"])
    #cargar_zip_ftp(path_local_fep,path_ftp_fep,id_lote,shared_properties,conn_str)




def cargar_zip_ftp(path_local_fep,path_ftp_fep,id_lote,shared_properties,conn_str):
    print("Cargando archivo")
    Conexion = Ftp_connection(int(shared_properties["port_ftp_siesa"]),shared_properties["server_ftp_siesa"],shared_properties["user_ftp_siesa"],shared_properties["path_key_file_ftp_siesa"])
    
    respuesta = Conexion.connect_with_keyfile()
    if respuesta:
        try:

            array_path_ftp=path_ftp_fep.split("/")
            del array_path_ftp[-1]
            path_directory_zip="/".join(array_path_ftp)


            Conexion.ensure_sftp_directory_exists(path_directory_zip)
           
            Conexion.ftp.put(path_local_fep, path_ftp_fep)
            

            remote_file_info = Conexion.ftp.stat(path_ftp_fep)
            local_file_size = os.path.getsize(path_local_fep)
            remote_file_size = remote_file_info.st_size

            if local_file_size == remote_file_size:
                print("Prueba exitosa: El archivo se cargó correctamente.")
                execute_update_query("sp_update_lote_estado_bscs",conn_str,[id_lote,2,"FEP"])
            else:
                print(f"Error: Tamaño del archivo local ({local_file_size} bytes) y remoto ({remote_file_size} bytes) no coinciden.")
        except Exception as e:
            print(f"An error occurred during file upload validation: {e}")
        finally:
            Conexion.disconnect_sftp()




def generartion_fep(id_corte, conn_str):
    try:

        manager=Manager()

        shared_properties = manager.dict()

        shared_properties["port_ftp_siesa"]=properties.port_ftp_siesa
        shared_properties["server_ftp_siesa"]=properties.server_ftp_siesa
        shared_properties["user_ftp_siesa"]=properties.user_ftp_siesa
        shared_properties["path_key_file_ftp_siesa"]=properties.path_key_file_ftp_siesa


        #validate_or_create_directory_for_file(ruta_local)
        conn = pyodbc.connect(conn_str)
        logger.info("Conexión establecida correctamente.")
        cursor = conn.cursor()

        consulta_lotes = "sp_listar_lotes_bscs " + str(id_corte)
        cursor.execute(consulta_lotes)
        resultados_lotes = cursor.fetchall()
        logger.info("Consulta de lotes ejecutada correctamente.")

        processes = []
        for lote in resultados_lotes:


            consulta_factura = f"sp_list_generacion_fep_bscs {str(lote[0])},{id_corte}" 
            logger.info("Iniciando consulta para lote: %s", lote[0])
            cursor.execute(consulta_factura)
            resultado_facturas = cursor.fetchall()


            logger.info("Consulta realizada para lote: %s", lote[0])

            id_lote=lote[0]
            path_local_fep=lote[4]
            path_ftp_fep=lote[5]
            


            p =Process(target=crear_zip, args=(resultado_facturas, path_local_fep,path_ftp_fep,id_lote,shared_properties,conn_str))
            processes.append(p)
            p.start()
            # break
      

        for p in processes:
            p.join()

        logger.info("Proceso completado correctamente.")
    except pyodbc.Error as e:
        logger.error("Error de base de datos: %s", e)
        print(e)
    except Exception as ex:
        logger.error("Error inesperado: %s", ex)
        print(e)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()






def prueba_bgh(conn_str):
    try:
        #validate_or_create_directory_for_file(ruta_local)
        conn = pyodbc.connect(conn_str)
        logger.info("Conexión establecida correctamente.")
        cursor = conn.cursor()

       
        

       
        consulta_factura = "sp_list_generacion_fe_bscs " + str(105)
        cursor.execute(consulta_factura)
        resultado_facturas = cursor.fetchall()


        for factura in resultado_facturas:

            factura_data=factura[0]
            leer_factura(factura_data)

    except pyodbc.Error as e:
        logger.error("Error de base de datos: %s", e)
    except Exception as ex:
        logger.error("Error inesperado: %s", ex)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()




def leer_factura(data):

    lineas=data.splitlines()


    for linea in lineas:
        print(linea)
    # try:


    #                 id_corte=2    
    #                 tag_11302 = tag_11303 = tag_11304 = tag_11305 = False

    #                 lineas=data.splitlines()
            



    #                 workTable_lotes = DataTable()

    #                 # Añadir las columnas al DataTable
    #                 workTable_lotes.Columns.Add("id", Type.GetType("System.Int32"))             # int
    #                 workTable_lotes.Columns.Add("id_corte", Type.GetType("System.Int32"))       # int
    #                 workTable_lotes.Columns.Add("num_lote", Type.GetType("System.Int32"))       # int
    #                 workTable_lotes.Columns.Add("nombre_lote", Type.GetType("System.String"))   # varchar
    #                 workTable_lotes.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))    # datetime
    #                 workTable_lotes.Columns.Add("estado", Type.GetType("System.Int32"))         # int


    #                 # Crear la tabla workTable_facturas
    #                 workTable_facturas = DataTable()

    #                 #workTable_facturas.Columns.Add("id", Type.GetType("System.Int32"))                # int
    #                 workTable_facturas.Columns.Add("id_lote", Type.GetType("System.Int32"))           # int
    #                 workTable_facturas.Columns.Add("num_factura", Type.GetType("System.Int64"))       # int64 para almacenar números grandes
    #                 workTable_facturas.Columns.Add("custcode", Type.GetType("System.String"))         # varchar
    #                 workTable_facturas.Columns.Add("tmcode", Type.GetType("System.String"))           # varchar
    #                 workTable_facturas.Columns.Add("data", Type.GetType("System.String"))             # varchar
    #                 workTable_facturas.Columns.Add("saldo_anterior", Type.GetType("System.Decimal"))  # decimal
    #                 workTable_facturas.Columns.Add("valor_total_pagar", Type.GetType("System.Decimal"))# varchar
    #                 workTable_facturas.Columns.Add("referencia_pago", Type.GetType("System.String"))  # varchar
    #                 workTable_facturas.Columns.Add("num_celular", Type.GetType("System.String"))  
    #                 workTable_facturas.Columns.Add("num_cuentas", Type.GetType("System.Int32"))       # varchar
    #                 workTable_facturas.Columns.Add("fecha_factura", Type.GetType("System.DateTime"))  # datetime
    #                 workTable_facturas.Columns.Add("fecha_limite_pago", Type.GetType("System.DateTime"))# datetime
    #                 workTable_facturas.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))       # datetime
    #                 workTable_facturas.Columns.Add("estado", Type.GetType("System.Int32"))            # int


    #                 num_cuentas=0
    #                 count_lote = 1
    #                 first_iteration=False
                    
    #                 contador_facturas_total = 0
    #                 contador_facturas_lote=0
    #                 tm_code=None
    #                 referencia_CODEpAIDMEN=None

       
      
    #                 id_lote=1
            
                   
                    

                    
    #                 contenido_factura = []
    #                 array_facturas = []
    #                 guardar_contenido = False
            
               
    #                 futures=[]
    #                 for linea in lineas:
    #                     # Inicializar campos
                    
    #                     if linea.startswith("11 "):
    #                         invoiceNumber = linea[368:378].strip()
    #                         cus_code = linea[378:413].strip()
    #                         numero_celular = linea[413:433].strip()  # Asumo que aquí debe haber un rango específico
    #                         code_payment=linea[537:539].strip()
    #                         invoiceDate=linea[521:529].strip()
                        

    #                     if linea.startswith("10 "):
    #                         guardar_contenido = True

                  
                           
                    
    #                     if linea.startswith("1110 "):
    #                         saldo_anterior = linea[44:62].strip()
    #                         valor_total_pagar = linea[116:134].strip()



    #                     if linea.startswith("1130 "):
                            
    #                         array_1130 = linea.split(";")
                           
    #                         fecha_limite_pago=linea[75:83].strip()

    #                         if code_payment == "CC":
    #                             referencia_CODEpAIDMEN = array_1130[1].strip()
    #                         else:
    #                             end_pos = linea.find(";", 101)
    #                             if end_pos != -1:
    #                                 referencia_CODEpAIDMEN = linea[101:end_pos].strip()
    #                             else:
    #                                  referencia_CODEpAIDMEN = linea[101:].strip()


    #                     if linea.startswith("11302 "):
    #                          tag_11302=True
    #                          referencia_11302=linea[8:].split("|")[0]
                             
    #                     if linea.startswith("11303 "):
    #                          tag_11303=True

    #                     if linea.startswith("11304 "):
    #                         tag_11304=True
    #                         referencia_11304=linea[8:].split("|")[0]

    #                     if linea.startswith("11305 "):
    #                         tag_11305=True
    #                         referencia_11305=linea[8:].split("|")[0]


                        


                        







                        

    #                     if linea.startswith("11399 "):
    #                         referencia_pago=linea[8:].split("|")[0]
                        
    #                     if linea.startswith("1400 "):
    #                         num_cuentas+=1

    #                     if linea.startswith("14051 "):
    #                         tm_code = linea[6:13].strip()
                        
                        
    #                     if guardar_contenido: 
    #                         contenido_factura.append(linea)
                            
    #                     if linea.startswith("99 "):  # Fin de una factura

    #                         #regla 1
    #                         if tag_11302 and tag_11303 and tag_11304:
    #                             referencia_pago=referencia_11302

    #                         #regla 2
    #                         if tag_11304 and not tag_11302 and not tag_11303:
    #                              referencia_pago = referencia_11304

    #                         #regla 3
    #                         if not tag_11302 and not tag_11303 and (not tag_11304 or not tag_11305) and referencia_CODEpAIDMEN is not None:
    #                             referencia_pago=referencia_CODEpAIDMEN
                            
    #                         #Regla 4
    #                         if tag_11302 and tag_11303 and tag_11305:
    #                               referencia_pago=referencia_11302
    #                         #Regla 5
    #                         if tag_11305 and not tag_11302 and not tag_11303:

    #                             referencia_pago=referencia_11305



    #                         if guardar_contenido:
    #                             if first_iteration==False:
    #                                 lote=f""
                                  
                                 
    #                                 first_iteration=True
                                
    #                             contador_facturas_total += 1
    #                             contador_facturas_lote +=1
                                
    #                             workTable_facturas.Rows.Add([id_lote,float(invoiceNumber),cus_code,tm_code,"".join(contenido_factura),float(saldo_anterior),float(valor_total_pagar),referencia_pago,numero_celular,num_cuentas,data_formatting.formatearFechaGuiones(invoiceDate),None,None,0])
    #                             num_cuentas=0

                                
                               
                            


    #                             if contador_facturas_total % 20000== 0:
    #                                 contador_facturas_lote =0
                                  
    #                                 count_lote += 1
    #                                 id_lote+=1
    #                                 lote=f""
                                    
    #                                 workTable_lotes.Rows.Add([None,id_corte,count_lote,lote,None,0])
                       
                                
                        
                                 

                                    
                                
                                


                                
    #                             contenido_factura = []
    #                             # Vaciar la lista después de procesar cada factura
    #                             guardar_contenido = False
    #                             invoiceNumber = cus_code = numero_celular = saldo_anterior = valor_total_pagar = referencia_pago = tm_code =fecha_limite_pago=referencia_CODEpAIDMEN=referencia_11305=code_payment=invoiceDate=None
    #                             tag_11302 = tag_11303 = tag_11304 = tag_11305 = False
    #                             workTable_lotes.Clear()
    #                             workTable_facturas.Clear()


    
                 

               

    

    # except Exception as e:
    #             print(str(e))
    #             print(invoiceNumber+"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    #             time.sleep(4)
       
