# import clr  # Importa el módulo de pythonnet

# # Carga el ensamblado de ADO.NET para SQL Server
# clr.AddReference("System.Data")
# from System.Data import SqlClient
# import System
# from System.Data import DataTable
import pyodbc


def truncate_table(table, conn_str):
    connection = None
    try:
        # Establecer la conexión utilizando pyodbc
        connection = pyodbc.connect(conn_str)

        # Crear el comando para ejecutar el TRUNCATE TABLE
        command = connection.cursor()
        command.execute(f"TRUNCATE TABLE {table}")
        connection.commit()  # Confirmar la transacción

        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        # Cerrar la conexión
        if connection:
            connection.close()



def execute_get_parameters_db(conn_str, parameters=None):
    connection = None
    proc_name="sp_get_parametros_config_bscs"
    try:
        # Establecer la conexión utilizando pyodbc
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        # Convertir lista de parámetros a una cadena formateada para SQL
        parameters_str = ','.join(parameters) if parameters else ''

        # Construir la llamada al procedimiento almacenado
        if parameters_str:
            query = f"EXEC {proc_name} @parametros = '{parameters_str}'"
        else:
            query = f"EXEC {proc_name}"

        # Ejecutar el procedimiento almacenado
        cursor.execute(query)

        # Recuperar resultados si es necesario
        while cursor.nextset():
            try:
                results = cursor.fetchall()
                return results
            except pyodbc.ProgrammingError:
                continue

        cursor.close()

    except Exception as e:
        print(f"Error al ejecutar el procedimiento almacenado '{proc_name}': {e}")

    finally:
        if connection:
            connection.close()


def execute_stored_procedure(proc_name, conn_str, parameters=None):
    connection = None

    try:
        # Establecer la conexión utilizando pyodbc
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        # Construir la llamada al procedimiento almacenado con parámetros
        if parameters:
            placeholders = ','.join(['?'] * len(parameters))
            query = f"EXEC {proc_name} {placeholders}"
            cursor.execute(query, parameters)
        else:
            query = f"EXEC {proc_name}"
            cursor.execute(query)

        # Obtener los resultados
        result = cursor.fetchall()
        cursor.close()

        return result

    except Exception as e:
        print(f"Error al ejecutar el procedimiento almacenado '{proc_name}': {e}")
        return None

    finally:
        if connection:
            connection.close()



def execute_update_query(query, conn_str, parameters=None):
    connection = None

    try:
        # Establecer la conexión utilizando pyodbc
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        # Construir la consulta UPDATE con parámetros
        if parameters:
            placeholders = ','.join(['?'] * len(parameters))
            query = f"{query} {placeholders}"

        # Ejecutar la consulta UPDATE con parámetros
        cursor.execute(query, parameters)

        # Obtener el número de filas actualizadas
        updated_rows = cursor.rowcount
        connection.commit()  # Confirmar la transacción

        return updated_rows  # Retorna el número de filas actualizadas

    except Exception as e:
        print(f"Error al ejecutar la consulta UPDATE: {str(e)}")
        return 0  # En caso de error, retornar 0 o manejar según tu aplicación

    finally:
        if connection:
            connection.close()
