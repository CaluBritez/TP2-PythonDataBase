import MySQLdb
import pandas as pd

# Leer el archivo CSV usando pandas
try:
    df = pd.read_csv("localidades.csv")
except FileNotFoundError:
    print("No se encontró el archivo localidades.csv.")
    exit()

HOST = 'localhost'
USUARIO = 'root'
CONTRASENA = ''
BASE_DATOS = 'prueba-python'

# Función para conectar a la base de datos MySQL
def conectar_mysql(host, usuario, contrasena, base_datos):
    try:
        conexion = MySQLdb.connect(
            host=host,
            user=usuario,
            passwd=contrasena,
            db=base_datos
        )
        print("Conexión exitosa a MySQL.")
        return conexion
    except MySQLdb.Error as error:
        print("Error al conectarse a MySQL:", error)
        return None

