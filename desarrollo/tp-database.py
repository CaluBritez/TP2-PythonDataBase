import MySQLdb
import csv
import sys

# Leer el archivo CSV utilizando la biblioteca csv
try:
    with open("localidades.csv", newline='') as archivo_csv:
        lector_csv = csv.reader(archivo_csv)
        campos = next(lector_csv)
        print(campos)
        # Leer los datos del archivo CSV y almacenarlos en una lista de tuplas
        datos = [fila for fila in lector_csv]
except FileNotFoundError:
    print("No se encontró el archivo localidades.csv")
    sys.exit(1)

#Datos para la conexion a la base de datos
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
        sys.exit(1)
    
# Comprobar la conexión a la base de datos
conexion = conectar_mysql(HOST, USUARIO, CONTRASENA, BASE_DATOS)

def crear_tabla(conexion, campos):
    try:
        cursor = conexion.cursor()
        # Crear la tabla con los campos de la lista
        sql = "CREATE TABLE IF NOT EXISTS localidades ("
        for campo in campos:
            sql += f"{campo} VARCHAR(255), "
    
        # Eliminar la última coma y espacio
        sql = sql[:-2]
        sql += ")"
        print(sql)
        cursor.execute(sql)
        print("Tabla 'localidades' creada correctamente.")
        conexion.commit()
        # conexion.close()
    except MySQLdb.Error as error:
        print("Error al crear la tabla:", error)

# Crear la tabla en la base de datos con los campos de la lista
crear_tabla(conexion, campos)

# Función para insertar los datos en la tabla localidades
def insertar_datos(conexion, datos):
    try:
        cursor = conexion.cursor()
        # Generar la sentencia INSERT INTO dinámicamente
        placeholders = ', '.join(['%s'] * len(campos))
        sql = f"INSERT INTO localidades ({', '.join(campos)}) VALUES ({placeholders})"
        print(sql)
        # Ejecutar la sentencia INSERT INTO para insertar los datos en la tabla
        cursor.executemany(sql, datos)
        print("Datos insertados correctamente.")
        conexion.commit()
    except MySQLdb.Error as error:
        print("Error al insertar datos:", error)

# Insertar los datos en la tabla localidades
insertar_datos(conexion, datos)

# Cerrar la conexión a la base de datos MySQL
conexion.close()