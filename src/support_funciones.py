# Librería para trabajar con bases de datos SQL
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2 import sql

def crear_basedatos(host,user,password,port,nombre_basedatos):
    conexion = psycopg2.connect(
        host=host,       
        user=user,      
        password=password, 
        port=port
    )


    conexion.autocommit = True  
    cursor = conexion.cursor()

    nombre_base_datos = nombre_basedatos

    try:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(nombre_base_datos)))
    except psycopg2.Error as e:
        print("Error de tipo:", e)

def conexion_a_dbeaver(database):
    """
    Establece una conexión a una base de datos DBeaver.

    Args:
        database (str): El nombre de la base de datos.

    Returns:
        conexion: Un objeto de conexión a la base de datos.
    """
  
    try:
        conexion = psycopg2.connect(
            database=database,
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )
    except OperationalError as e:
        if e.pgcode == errorcodes.INVALID_PASSWORD:
            print("La contraseña intorducida es errónea")
        elif e.pgcode == errorcodes.CONNECTION_EXCEPTION:
            print("Se ha producido un error de conexión")
        else:
            print(f"Ocurrió el error {e}")
    return conexion




def insercion_datos(conexion, cursor, lista_tuplas, query_insercion):
    cursor.executemany(query_insercion, lista_tuplas)
    conexion.commit() 