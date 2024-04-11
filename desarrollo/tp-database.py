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

# Función para obtener las distintas provincias de la tabla localidades
def obtener_provincias(conexion):
    try:
        cursor = conexion.cursor()
        cursor.execute("SELECT DISTINCT provincia FROM localidades")
        provincias = [fila[0] for fila in cursor.fetchall()]
        return provincias
    except MySQLdb.Error as error:
        print("Error al obtener las provincias:", error)
        sys.exit(1)
    finally:
        cursor.close()

# Obtener las provincias de la tabla localidades
provincias = obtener_provincias(conexion)
print("Provincias encontradas:", provincias)

# Función para obtener las localidades de una provincia
def obtener_localidades_por_provincia(conexion, provincia):
    try:
        cursor = conexion.cursor()
        cursor.execute("SELECT localidad FROM localidades WHERE provincia = %s", (provincia,))
        localidades = [fila[0] for fila in cursor.fetchall()]
        return list(localidades)
    except MySQLdb.Error as error:
        print(f"Error al obtener las localidades de {provincia}:", error)
        sys.exit(1)
    finally:
        cursor.close()

# Función para obtener un diccionario con las localidades por provincia
def obtener_localidades_por_provincias(conexion, provincias):
    localidades_por_provincia = {}
    for provincia in provincias:
        localidades = obtener_localidades_por_provincia(conexion, provincia)
        localidades_por_provincia[provincia] = localidades
    return localidades_por_provincia

# Obtener las localidades por provincia
localidades_por_provincia = obtener_localidades_por_provincias(conexion, provincias)
#print("Localidades por provincia:", localidades_por_provincia)
#for valor in localidades_por_provincia.keys():
#   print(valor)
def eliminar_repetidos(diccionario):
    diccionario_sin_repetidos = {}

    for provincia, localidades in diccionario.items():
        localidades_unicas = list(set(localidades))  # Convertir a conjunto para eliminar repetidos y luego volver a convertir a lista
        diccionario_sin_repetidos[provincia] = localidades_unicas

    return diccionario_sin_repetidos

diccionario_sin_repetidos = eliminar_repetidos(localidades_por_provincia)

def crear_archivos_csv(diccionario):
    for provincia, localidades in diccionario.items():
        nombre_archivo = f"{provincia}.csv"
        with open(nombre_archivo, 'w', newline='') as archivo_csv:
            escritor_csv = csv.writer(archivo_csv)
            escritor_csv.writerow(["Localidades de " + provincia])  # Escribir encabezado
            for localidad in localidades:
                escritor_csv.writerow([localidad])

# Utilizar la función para crear los archivos CSV
crear_archivos_csv(diccionario_sin_repetidos)

# Cerrar la conexión a la base de datos MySQL
conexion.close()
