import sys
import MySQLdb
try:
    db = MySQLdb.connect("localhost","root","","prueba-python" )
except MySQLdb.Error as e:
    print("No puedo conectar a la base de datos:",e)
    sys.exit(1)
cursor = db.cursor()
# Borrar la tabla
cursor.execute("DROP TABLE IF EXISTS empleados")
print("Tabla 'empleados' eliminada (si existía).")
cursor.execute("""

CREATE TABLE empleados (
id INT AUTO_INCREMENT PRIMARY KEY,
nombre VARCHAR(50),
apellido VARCHAR(50),
edad INT,
sexo CHAR(1),
ingreso DECIMAL(10, 2)
)
""")
print("Tabla 'empleados' creada exitosamente.")
sql = "INSERT INTO empleados (nombre, apellido, edad, sexo, ingreso) VALUES (%s, %s, %s, %s, %s)"
values = ('José', 'Domingo', 20, 'M', 2000)
try:
    cursor.execute(sql, values)
    db.commit()
    print("Insertado correctamente")
except Exception as e:
    print("Error al insertar los datos", e)
    db.rollback()
db.close()