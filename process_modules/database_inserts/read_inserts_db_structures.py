import clr
clr.AddReference('System.Data')
from System.Data import DataTable  
from System import Type
from datetime import datetime  
from log_book import logger_config
from database.save_database import save_database
import gzip
from process_modules.data_formatting import data_formatting
from database import queries
import concurrent.futures
from config.config import properties
def insert_base_email_file(path_local,id_corte,conn_str,conn_str_db_pyodbc):

    # Limpiar tabla de insumo de correo electronico para insertar nuevo insumo 
    queries.truncate_table("tbl_insumos_base_email_bscs",conn_str_db_pyodbc)
    
    # Crear la tabla workTable
    workTable_base_email = DataTable()

    # Añadir las columnas al DataTable
    workTable_base_email.Columns.Add("custcode", Type.GetType("System.String"))        # varchar
    workTable_base_email.Columns.Add("detalle", Type.GetType("System.String"))         # nvarchar
    workTable_base_email.Columns.Add("correo", Type.GetType("System.String"))          # varchar
    workTable_base_email.Columns.Add("id_corte", Type.GetType("System.Int32"))         # int
    workTable_base_email.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))      # datetime


    contador_avisos_ascard=1
    primera_linea=1

    correo=""
    with open(path_local, 'r', encoding='latin-1') as archivo:
         for linea in archivo:
              if primera_linea==1:
                  primera_linea+=1  
                  continue
              cust_code=linea.split(",")[0]
              if len(linea.split())>=4:
                  correo=linea.split(",")[3]
              else: correo=""
             
              workTable_base_email.Rows.Add([cust_code,linea.strip(),correo.strip(),id_corte,None])
              if contador_avisos_ascard%10000==0:
                 save_database(workTable_base_email,'tbl_insumos_base_email_bscs',conn_str)
                 print("Insertado 1 millon")
                 print(str(datetime.now()))
                 workTable_base_email.Clear()
              contador_avisos_ascard+=1


         save_database(workTable_base_email,'tbl_insumos_base_email_bscs',conn_str)



def insert_database_retenciones_file(path,period,id_corte,conn_str):


    workTable_retenciones = DataTable()

    # Añadir las columnas al DataTable
    workTable_retenciones.Columns.Add("id_corte", Type.GetType("System.Int32"))          # int
    workTable_retenciones.Columns.Add("periodo", Type.GetType("System.String"))         # varchar
    workTable_retenciones.Columns.Add("custcode", Type.GetType("System.String"))         # varchar
    workTable_retenciones.Columns.Add("detalle", Type.GetType("System.String"))          # varchar
    workTable_retenciones.Columns.Add("tipo_retencion", Type.GetType("System.Int32"))    # int
    workTable_retenciones.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))       # datetime



  
    counter_reten = 0
    
    try:
        with open(path, 'r', encoding='latin-1') as file:
            for line in file:
                array_line = line.split("|")
               
                if len(array_line) > 1:
                   
                    workTable_retenciones.Rows.Add([id_corte,None,array_line[0].strip(),line.strip(),0,None])
                else:
                    workTable_retenciones.Rows.Add([id_corte,period,array_line[0].strip(),line.strip(),1,None])
      
                counter_reten += 1

                
                if counter_reten % 20000 == 0:
                 
                    save_database(workTable_retenciones, "tbl_insumos_base_retenciones_bscs",conn_str)
                    
            save_database(workTable_retenciones, "tbl_insumos_base_retenciones_bscs",conn_str)
    
       
    except Exception as e:
        logger_config.logger.error(f"Error durante el proceso de lectura y carga a la base de datos del Insumo: Retencion:{e}")
        raise Exception("Se ha producido un error: "+e)





def insert_database_contratos_file(local_path,id_corte,conn_str):
    try:
        # Crear la tabla workTable
        workTable_contratos = DataTable()
        # Añadir las columnas al DataTable
        workTable_contratos.Columns.Add("custcode", Type.GetType("System.String"))       # int
        workTable_contratos.Columns.Add("detalle", Type.GetType("System.String"))          # nvarchar
        workTable_contratos.Columns.Add("id_corte", Type.GetType("System.Int32"))          # int
        workTable_contratos.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))       # datetime

        counter_contratos = 0
        logger_config.logger.info("Inicio del proceso de lectura y carga a la base de datos de contratos: {file_name}")
        
        with open(local_path, "r") as file:
            for linea in file:
                counter_contratos += 1
                custcode = linea.split("|")[21]
                workTable_contratos.Rows.Add([custcode, linea.strip(),id_corte,None])

                if counter_contratos % 20000 == 0:
                    logger_config.logger.info(f"Almacenando en la base de datos, Contratos procesados hasta ahora: {counter_contratos}.")
                    save_database(workTable_contratos, 'tbl_insumos_contratos_bscs',conn_str)
                    
            save_database(workTable_contratos, "tbl_insumos_contratos_bscs",conn_str)

        logger_config.logger.info(f"Finalización del proceso de lectura y carga a la base de datos de contratos. Contratos insertados: {counter_contratos}")
        
    except Exception as e:
        logger_config.logger.error(f"Error durante el proceso de lectura y carga a la base de datos de contratos: {e}")
        raise Exception("Se ha producido un error: "+e)



def insert_database_aviso_servicios_file(local_path,id_corte,conn_str):
    try:

        # Crear la tabla workTable
        workTable_servicios = DataTable()

        # Añadir las columnas al DataTable
                    # int
        workTable_servicios.Columns.Add("custcode", Type.GetType("System.String"))         # varchar
        workTable_servicios.Columns.Add("detalle", Type.GetType("System.String"))          # nvarchar
        workTable_servicios.Columns.Add("id_corte", Type.GetType("System.Int32"))          # int
        workTable_servicios.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))       # datetime

        contador_avisos = 0
        primera_linea=1
        logger_config.logger.info("Inicio del proceso de lectura y carga a la base de datos de aviso servico")
        
        with open(local_path, "r") as file:
            for linea in file:
                if primera_linea == 1:
                    primera_linea += 1
                    continue
                contador_avisos += 1
                
                custcode=linea.strip()[:10]
                workTable_servicios.Rows.Add([custcode,linea.strip(),id_corte,None])
                if contador_avisos % 20000 == 0:
                     logger_config.logger.info("Almacenando en la base de datos los avisos_servicios procesados hasta ahora.")
                     save_database(workTable_servicios, "dbo.tbl_insumos_avisos_servicios_bscs",conn_str)
                    
            save_database(workTable_servicios, "dbo.tbl_insumos_avisos_servicios_bscs",conn_str)

        logger_config.logger.info("Finalización del proceso de lectura y carga a la base de datos de avisos_servicios")
        
    except Exception as e:
         logger_config.logger.error(f"Error durante el proceso de lectura y carga a la base de datos de avisos_servicios: {e}")
         raise Exception("Se ha producido un error: "+e)



def insert_aviso_equipo_ascard_file(local_path,id_corte,conn_str):
    try:

        # Crear la tabla workTable
        workTable_aviso_ascard = DataTable()

        # Añadir las columnas al DataTable
        workTable_aviso_ascard.Columns.Add("custcode_ascard", Type.GetType("System.String"))   # varchar
        workTable_aviso_ascard.Columns.Add("custcode_serv", Type.GetType("System.String"))     # varchar
        workTable_aviso_ascard.Columns.Add("detalle", Type.GetType("System.String"))           # nvarchar
        workTable_aviso_ascard.Columns.Add("id_corte", Type.GetType("System.Int32"))           # int
        workTable_aviso_ascard.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))        # datetime

        contador_avisos_ascard = 0
        primera_linea = 1
        logger_config.logger.info("Inicio del proceso de lectura y carga a la base de datos de avisos ASCARD")
        
        with open(local_path, "r") as file:
            for linea in file:
                if primera_linea == 1:
                    primera_linea += 1
                    continue
                contador_avisos_ascard += 1
                array_cust_codes = linea[:21].strip().split()
                workTable_aviso_ascard.Rows.Add([array_cust_codes[0], array_cust_codes[1], linea.strip(),id_corte,None])
                
                if contador_avisos_ascard % 20000 == 0:
                    logger_config.logger.info("Almacenando en la base de datos los avisos ASCARD procesados hasta ahora.")
                    save_database(workTable_aviso_ascard, "tbl_insumos_avisos_equipos_ascard_bscs",conn_str)  

            save_database(workTable_aviso_ascard, "tbl_insumos_avisos_equipos_ascard_bscs",conn_str)      
            logger_config.logger.info("Finalización del proceso de lectura y carga a la base de datos de avisos ASCARD")
            
    
    except Exception as e:
        logger_config.logger.error(f"Error durante el proceso de lectura y carga a la base de datos de avisos ASCARD: {e}")
        raise Exception("Se ha producido un error: "+e)



def insert_aviso_normal_file(local_path,id_corte,conn_str):
    try:
        workTable_aviso_eq_normal = DataTable()

        # Añadir las columnas al DataTable
        workTable_aviso_eq_normal.Columns.Add("custcode_eq", Type.GetType("System.String"))      # varchar
        workTable_aviso_eq_normal.Columns.Add("custcode_serv", Type.GetType("System.String"))    # varchar
        workTable_aviso_eq_normal.Columns.Add("detalle", Type.GetType("System.String"))          # nvarchar
        workTable_aviso_eq_normal.Columns.Add("id_corte", Type.GetType("System.Int32"))          # int
        workTable_aviso_eq_normal.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))       # datetime


        contador_avisos = 0
        primera_linea = 1
        logger_config.logger.info("Inicio del proceso de lectura y carga a la base de datos de avisos normales")
        
        with open(local_path, "r") as file:
            for linea in file:
                if primera_linea == 1:
                    primera_linea += 1
                    continue
                contador_avisos += 1
                array_cust_codes = linea[:21].strip().split() 
                workTable_aviso_eq_normal.Rows.Add([array_cust_codes[0], array_cust_codes[1], linea.strip(),id_corte,None])
                
                if contador_avisos % 20000 == 0:
                    logger_config.logger.info("Almacenando en la base de datos los avisos procesados hasta ahora.")
                    save_database(workTable_aviso_eq_normal, "dbo.tbl_insumos_avisos_equipos_normal_bscs",conn_str)
                    
            save_database(workTable_aviso_eq_normal, "dbo.tbl_insumos_avisos_equipos_normal_bscs",conn_str)        
            logger_config.logger.info("Finalización del proceso de lectura y carga a la base de datos de avisos normales")
   
    
    except Exception as e:
        logger_config.logger.error(f"Error durante el proceso de lectura y carga a la base de datos de avisos normales: {e}")
        raise Exception("Se ha producido un error: "+e)


def insert_bgh_folios_file(local_path,id_corte,conn_str_pythonnet,conn_str_pyodbc,batch_id, paths_lotes):
        

      
        
        tag_11302 = tag_11303 = tag_11304 = tag_11305 = False

 
  



        workTable_lotes = DataTable()

        # Añadir las columnas al DataTable
        workTable_lotes.Columns.Add("id", Type.GetType("System.Int32"))             # int
        workTable_lotes.Columns.Add("id_corte", Type.GetType("System.Int32"))       # int
        workTable_lotes.Columns.Add("num_lote", Type.GetType("System.Int32"))       # int
        workTable_lotes.Columns.Add("nombre_lote", Type.GetType("System.String"))   # varchar
        workTable_lotes.Columns.Add("path_local_fe", Type.GetType("System.String"))   # varchar
        workTable_lotes.Columns.Add("path_ftp_fe", Type.GetType("System.String"))   # varchar
        workTable_lotes.Columns.Add("estado_fe", Type.GetType("System.Int32"))         # int
        workTable_lotes.Columns.Add("path_local_fep", Type.GetType("System.String"))   # varchar
        workTable_lotes.Columns.Add("path_ftp_fep", Type.GetType("System.String"))   # varchar
        workTable_lotes.Columns.Add("estado_fep", Type.GetType("System.Int32"))         # int
        workTable_lotes.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))    # datetime
       


        # Crear la tabla workTable_facturas
        workTable_facturas = DataTable()

        #workTable_facturas.Columns.Add("id", Type.GetType("System.Int32"))                # int
        workTable_facturas.Columns.Add("id_lote", Type.GetType("System.Int32"))           # int
        workTable_facturas.Columns.Add("num_factura", Type.GetType("System.Int64"))       # int64 para almacenar números grandes
        workTable_facturas.Columns.Add("custcode", Type.GetType("System.String"))         # varchar
        workTable_facturas.Columns.Add("tmcode", Type.GetType("System.String"))           # varchar
        workTable_facturas.Columns.Add("data", Type.GetType("System.String"))             # varchar
        workTable_facturas.Columns.Add("saldo_anterior", Type.GetType("System.Decimal"))  # decimal
        workTable_facturas.Columns.Add("valor_total_pagar", Type.GetType("System.Decimal"))# varchar
        workTable_facturas.Columns.Add("referencia_pago", Type.GetType("System.String"))  # varchar
        workTable_facturas.Columns.Add("num_celular", Type.GetType("System.String"))  
        workTable_facturas.Columns.Add("num_cuentas", Type.GetType("System.Int32"))       # varchar
        workTable_facturas.Columns.Add("fecha_factura", Type.GetType("System.DateTime"))  # datetime
        workTable_facturas.Columns.Add("fecha_limite_pago", Type.GetType("System.DateTime"))# datetime
        workTable_facturas.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))       # datetime
        workTable_facturas.Columns.Add("estado", Type.GetType("System.Int32"))            # int


        num_cuentas=0
        count_lote = 1
        first_iteration=False
        
        contador_facturas_total = 0
        contador_facturas_lote=0
        tm_code=None
        referencia_CODEpAIDMEN=None
        contador_lineas=0
        logger_config.logger.info("Inicio proceso lectura e insersion de  data BGH a base de datos")
        try:
            id_lote=int(queries.execute_stored_procedure("get_max_id_lote_bscs",conn_str_pyodbc)[0][0])
         
            [prefijo_lote_zip_archivosFtp,sistema_lote_zip_archivosFtp]=queries.execute_get_parameters_db(conn_str_pyodbc,parameters=["PrefijoLoteZipArchivosFtp","SistemaLoteZipArchivosFtp"])[0]
            
            celular_tag_11=False
            with gzip.open(local_path, 'rt') as file:
                logger_config.logger.info("Inicio del proceso de lectura del archivo local: " + local_path)
                contenido_factura = []
                array_facturas = []
                guardar_contenido = False
            
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                    futures=[]
                    for linea in file:
                        # Inicializar campos
                        contador_lineas+=1
                         # Inicializar campos
                        if linea.startswith("11 "):
                            invoiceNumber = linea[368:378].strip()
                            cus_code = linea[378:413].strip()
                             
                            code_payment = linea[537:539].strip()
                            invoiceDate = linea[521:529].strip()
                            if linea[413:433].strip()!="MAESTRA":
                                celular_tag_11=True
                                numero_celular =linea[413:433].strip()

                        if linea.startswith("10 "):
                            guardar_contenido = True

                        if linea.startswith("1110 "):
                            saldo_anterior = linea[44:62].strip()
                            valor_total_pagar = linea[116:134].strip()

                        if linea.startswith("1130 "):
                            array_1130 = linea.split(";")
                            #fecha_limite_pago = linea[75:83].strip()

                            if code_payment == "CC":
                                referencia_CODEpAIDMEN = array_1130[1].strip()
                            else:
                                end_pos = linea.find(";", 101)
                                if end_pos != -1:
                                    referencia_CODEpAIDMEN = linea[101:end_pos].strip()
                                else:
                                    referencia_CODEpAIDMEN = linea[101:].strip()

                        if linea.startswith("11302 "):
                            tag_11302 = True
                            referencia_11302 = linea[8:].split("|")[0]

                        if linea.startswith("11303 "):
                            tag_11303 = True

                        if linea.startswith("11304 "):
                            tag_11304 = True
                            referencia_11304 = linea[8:].split("|")[0]

                        if linea.startswith("11305 "):
                            tag_11305 = True
                            referencia_11305 = linea[8:].split("|")[0]

                        if linea.startswith("11399 "):
                            referencia_pago = linea[8:].split("|")[0]

                        if linea.startswith("1400 "):
                            num_cuentas += 1
                            if num_cuentas==1 and not celular_tag_11:
                               numero_celular=linea[78:96].strip()

                        if linea.startswith("14051 "):
                            tm_code = linea[6:13].strip()

                        if guardar_contenido:
                            contenido_factura.append(linea)

                        if linea.startswith("99 "):  # Fin de una factura

                            

                            # Regla 1
                            if tag_11302 and tag_11303 and tag_11304:
                                referencia_pago = referencia_11302

                            # Regla 2
                            if tag_11304 and not tag_11302 and not tag_11303:
                                referencia_pago = referencia_11304

                            # Regla 3
                            if not tag_11302 and not tag_11303 and (not tag_11304 or not tag_11305) and referencia_CODEpAIDMEN is not None:
                                referencia_pago = referencia_CODEpAIDMEN

                            # Regla 4
                            if tag_11302 and tag_11303 and tag_11305:
                                referencia_pago = referencia_11302

                            #regla 5
                            if tag_11305 and not tag_11302 and not tag_11303:
                                referencia_pago = referencia_11305

                            if guardar_contenido:
                                if first_iteration==False:

                                    lote=f"{prefijo_lote_zip_archivosFtp}{batch_id}_{sistema_lote_zip_archivosFtp}_{data_formatting.get_current_date()}_{data_formatting.format_to_2_digits(count_lote)}"
                                    path_local_FE=f"{paths_lotes[1]}{lote}.zip"
                                    path_ftp_FE=f"{paths_lotes[0]}{lote}.zip"
                                    path_local_FEP=f"{paths_lotes[3]}{lote}.zip"
                                    path_ftp_FEP=f"{paths_lotes[2]}{lote}.zip"

                                    

                                    workTable_lotes.Rows.Add([None,id_corte,count_lote,lote,path_local_FE,path_ftp_FE,0,path_local_FEP,path_ftp_FEP,0,None])
                                    save_database(workTable_lotes, "tbl_lote_bscs",conn_str_pythonnet)
                                    first_iteration=True
                                
                                contador_facturas_total += 1
                                contador_facturas_lote +=1
                                
                                workTable_facturas.Rows.Add([id_lote,float(invoiceNumber),cus_code,tm_code,"".join(contenido_factura),float(saldo_anterior),float(valor_total_pagar),referencia_pago,numero_celular,num_cuentas,data_formatting.formatearFechaGuiones(invoiceDate),None,None,0])
                                num_cuentas=0

                                
                                if contador_facturas_total % 500== 0:
                                    
                                    futures.append(executor.submit(save_database,workTable_facturas.Copy(),"tbl_factura_bscs",conn_str_pythonnet))
                                    # save_database(workTable_facturas, "tbl_factura_bscs",conn_str_pythonnet)
                                    workTable_facturas.Clear()
                                    for future in concurrent.futures.as_completed(futures):
                                        future.result()  # Process result or handle exception
                            


                                if contador_facturas_total % 20000== 0:
                                    contador_facturas_lote =0
                                    
                                    count_lote += 1
                                    id_lote+=1
                                    lote=f"{prefijo_lote_zip_archivosFtp}{batch_id}_{sistema_lote_zip_archivosFtp}_{data_formatting.get_current_date()}_{data_formatting.format_to_2_digits(count_lote)}"

                                    path_local_FE=f"{paths_lotes[1]}{lote}.zip"
                                    path_ftp_FE=f"{paths_lotes[0]}{lote}.zip"
                                    path_local_FEP=f"{paths_lotes[3]}{lote}.zip"
                                    path_ftp_FEP=f"{paths_lotes[2]}{lote}.zip"
                                    workTable_lotes.Rows.Add([None,id_corte,count_lote,lote,path_local_FE,path_ftp_FE,0,path_local_FEP,path_ftp_FEP,0,None])

                                    logger_config.logger.info(f"Insertando lote en la base de datos: {lote}")
                                    save_database(workTable_lotes, "tbl_lote_bscs",conn_str_pythonnet)
                        
                                    logger_config.logger.info(f"Insertando facturas del lote {lote} en la base de datos.")

                                    
                                
                                


                                
                                contenido_factura = []
                                # Vaciar la lista después de procesar cada factura
                                guardar_contenido=celular_tag_11= False
                                invoiceNumber = cus_code = numero_celular = saldo_anterior = valor_total_pagar = referencia_pago = tm_code=referencia_CODEpAIDMEN=referencia_11305=code_payment=invoiceDate=None
                                tag_11302 = tag_11303 = tag_11304 = tag_11305 = False


                if workTable_facturas:
                     
                    save_database(workTable_facturas, "tbl_factura_bscs",conn_str_pythonnet)
                
                properties.num_lineas_totales_corte=contador_lineas
                logger_config.logger.info("Fin del proceso de lectura del archivo.")

        except Exception as e:
            logger_config.logger.error(f"Error durante el proceso de lectura del archivo: {e}")
            print(str(e))
            raise Exception("Se ha producido un error: "+e)