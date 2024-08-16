import clr
from System import Type
clr.AddReference('System.Data')
from System.Data import DataTable  
from datetime import datetime  
from log_book import logger_config
from database import save_database


def insert_database_config(parameters,conn_str):

    work_table_configuracion=DataTable()

 
    work_table_configuracion.Columns.Add("id", Type.GetType("System.Int32"));                
    work_table_configuracion.Columns.Add("nombre", Type.GetType("System.String"));          
    work_table_configuracion.Columns.Add("valor", Type.GetType("System.String"));             
    work_table_configuracion.Columns.Add("fecha_ts", Type.GetType("System.DateTime"));     


    for param in parameters: 
        work_table_configuracion.Rows.Add([None,param.get("Nombre"),param.get("Valor"),None])

    
    
    save_database.save_database(work_table_configuracion,"tbl_configuraciones_bscs",conn_str)

