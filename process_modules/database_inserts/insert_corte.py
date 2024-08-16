import clr
from System import Type
clr.AddReference('System.Data')
from System.Data import DataTable  
from datetime import datetime  
from log_book import logger_config
from database.save_database import save_database






def insert_corte(conn_str,file_name,bach_id,corte,ciclo,periodo):

    # Crear una instancia de DataTable
    work_table_corte = DataTable("Corte")

    # Agregar columnas a la DataTable
    work_table_corte.Columns.Add("id", Type.GetType("System.Int32"))
    work_table_corte.Columns.Add("nombre_corte", Type.GetType("System.String"))
    work_table_corte.Columns.Add("batch_id", Type.GetType("System.String"))
    work_table_corte.Columns.Add("corte", Type.GetType("System.Int32"))
    work_table_corte.Columns.Add("ciclo", Type.GetType("System.String"))
    work_table_corte.Columns.Add("num_lineas_corte", Type.GetType("System.Int32"))
    work_table_corte.Columns.Add("num_folios_corte", Type.GetType("System.Int32"))
    work_table_corte.Columns.Add("num_facturas_corte", Type.GetType("System.Int32"))
    work_table_corte.Columns.Add("fecha_ts", Type.GetType("System.DateTime"))
    work_table_corte.Columns.Add("estado", Type.GetType("System.Int32"))
    work_table_corte.Columns.Add("periodo", Type.GetType("System.String"))



    work_table_corte.Rows.Add([corte,file_name,bach_id,corte,ciclo,None,None,None,None,0,periodo])

    save_database(work_table_corte,"tbl_corte_bscs",conn_str)







