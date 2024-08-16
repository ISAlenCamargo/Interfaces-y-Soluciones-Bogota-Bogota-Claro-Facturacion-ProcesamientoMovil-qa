import clr
from System import Type
clr.AddReference('System.Data')
from System.Data import DataTable  
from datetime import datetime  
from log_book import logger_config
from database.save_database import save_database




def insert_info_file(conn_str_db,file_name,tipo,file_extention,file_size,local_path,remote_path,estado,id_corte,periodo):

    work_table_archivos = DataTable("Archivos")

    # Agregar columnas a la DataTable
    work_table_archivos.Columns.Add("id", Type.GetType("System.Int32"))
    work_table_archivos.Columns.Add("nombre", Type.GetType("System.String"))
    work_table_archivos.Columns.Add("tipo", Type.GetType("System.String"))
    work_table_archivos.Columns.Add("extension", Type.GetType("System.String"))
    work_table_archivos.Columns.Add("tamanio", Type.GetType("System.Int32"))
    work_table_archivos.Columns.Add("path_local", Type.GetType("System.String"))
    work_table_archivos.Columns.Add("path_remoto", Type.GetType("System.String"))
    work_table_archivos.Columns.Add("estado", Type.GetType("System.Int32"))
    work_table_archivos.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))
    work_table_archivos.Columns.Add("id_corte", Type.GetType("System.Int32"))
    work_table_archivos.Columns.Add("periodo", Type.GetType("System.String"))


    work_table_archivos.Rows.Add([None,file_name,tipo,file_extention,file_size,local_path,remote_path,estado,None,id_corte,periodo])



    save_database(work_table_archivos,"tbl_archivo_bscs",conn_str_db)



    





