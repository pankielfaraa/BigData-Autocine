
from pyspark.sql import SparkSession

# 1. Iniciar Spark
spark = SparkSession.builder \
    .appName("Autocine Big Data") \
    .master("local[*]") \
    .getOrCreate()

print("Spark iniciado correctamente")

# 2. Definir la ruta de los archivos
ruta = "/opt/spark/work-dir/data/"

# 3. Leer las cuatro colecciones JSON
clientes = spark.read.json(ruta + "clientes.json")
peliculas = spark.read.json(ruta + "peliculas.json")
funciones = spark.read.json(ruta + "funciones.json")
entradas = spark.read.json(ruta + "entradas.json")

# 4. Mostrar los datos con DataFrames
print("\n=== CLIENTES ===")
clientes.show(5, truncate=False)

print("\n=== PELICULAS ===")
peliculas.show(10, truncate=False)

print("\n=== FUNCIONES ===")
funciones.show(5, truncate=False)

print("\n=== ENTRADAS ===")
entradas.show(5, truncate=False)

# 5. Mostrar la estructura de los datos
print("\n=== ESTRUCTURA DE PELICULAS ===")
peliculas.printSchema()

# 6. Procesar datos con RDD
print("\n=== RDD DE PELICULAS ===")
rdd = peliculas.rdd

for fila in rdd.take(5):
    print(fila)

# 7. Analizar películas con Spark SQL
print("\n=== PELICULAS POR GENERO ===")
peliculas.createOrReplaceTempView("peliculas")

resultado = spark.sql("""
    SELECT genero, COUNT(*) AS cantidad
    FROM peliculas
    GROUP BY genero
    ORDER BY cantidad DESC
""")

resultado.show()

# 8. Finalizar Spark
spark.stop()
