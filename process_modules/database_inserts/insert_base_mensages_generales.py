import clr
clr.AddReference('System.Data')
from System.Data import DataTable  
from System import Type
# from database.save_database import save_database
import clr
clr.AddReference('System.Data')
from System.Data import SqlClient


path_local_file="./data/ENTRADA/BASE_MENSAJES_MOVIL_SEPTIEMBRE_V1.csv"
#conexion_pytodbc="DRIVER=ODBC Driver 17 for SQL Server;SERVER=CNKT-DEV-FD;DATABASE=claro_connekta_qa;UID=aycamargo;PWD=Alen12345*;fast_executemany=True;"
#conexion_pythonnet="SERVER=CNKT-DEV-FD;DATABASE=claro_connekta_qa;User Id=aycamargo; Password=Alen12345*"

conexion_pythonnet = "SERVER=10.10.23.11;DATABASE=claro_connekta_core;User Id=connekta$;Password=XSkv@dxULr4r"
conexion_pytodbc="DRIVER=ODBC Driver 17 for SQL Server;SERVER=10.10.23.11;DATABASE=claro_connekta_core;UID=connekta$;PWD=XSkv@dxULr4r;fast_executemany=True;"
       
def save_database(data_table, destination_table, conn_str):
    sqlDbConnection = SqlClient.SqlConnection(conn_str)
    try:
        sqlDbConnection.Open()

        sbc = SqlClient.SqlBulkCopy(sqlDbConnection)
        sbc.DestinationTableName = destination_table
        sbc.BulkCopyTimeout = 0
        sbc.WriteToServer(data_table)
        data_table.Clear()
        return True  # Operación completada con éxito
    except SqlClient.SqlException as ex:
        print(ex)
        return False  # Error de SQL
    except Exception as ex:
        print(ex)
        return False  # Otro tipo de error
    finally:
        sqlDbConnection.Close()


def insert_file_mensajes_generales():
    # Crear la tabla workTable
    work_table_mensajes = DataTable()

    # Añadir las columnas al DataTable
    work_table_mensajes.Columns.Add("id_img", Type.GetType("System.String"))    # varchar
    work_table_mensajes.Columns.Add("custcode", Type.GetType("System.String"))    # varchar
    work_table_mensajes.Columns.Add("tmcode", Type.GetType("System.String"))      # varchar
    work_table_mensajes.Columns.Add("id_mensaje", Type.GetType("System.String"))  # varchar
    work_table_mensajes.Columns.Add("detalle", Type.GetType("System.String"))     # nvarchar
    work_table_mensajes.Columns.Add("fecha", Type.GetType("System.DateTime"))     # datetime

    contador_mensajes=0

    with open(path_local_file, 'r', encoding='latin-1') as archivo:
         
         for linea in archivo:
              contador_mensajes+=1

              array_linea_mensaje=linea.split("|")
              custcode=array_linea_mensaje[0]
              tmcode=array_linea_mensaje[1]
              id_mensaje=array_linea_mensaje[2]
              detalle=linea.strip()


              work_table_mensajes.Rows.Add([None,custcode,tmcode,id_mensaje,detalle,None])


              if contador_mensajes%20000==0:
                  
                  save_database(work_table_mensajes,"tbl_insumos_base_mensajes_movil_bscs",conexion_pythonnet)
                   
         save_database(work_table_mensajes,"tbl_insumos_base_mensajes_movil_bscs",conexion_pythonnet)
                   
              
              


insert_file_mensajes_generales()


