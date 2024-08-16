import clr
clr.AddReference('System.Data')
from System.Data import SqlClient
from log_book import logger_config
from datetime import datetime


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
         logger_config.logger.info(f"Excepción-save_database: {ex}")               
         print(ex)
         return False  # Error de SQL
     except Exception as ex:
         logger_config.logger.info(f"Excepción-save_database: {ex}")     
         print(ex)
         return False  # Otro tipo de error
     finally:
         sqlDbConnection.Close()

