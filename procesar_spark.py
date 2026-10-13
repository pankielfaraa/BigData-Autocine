
from pyspark.sql import SparkSession
from urllib.parse import quote_plus

# 1. Configurar la conexión con MongoDB Atlas
usuario = "difertota_db_user"
password = quote_plus("AutoCine2026Fernando123")

URL_MONGO = (
    f"mongodb+srv://{usuario}:{password}"
    "@cluster0.jprrr6g.mongodb.net/autocine_bigdata"
)

# 2. Iniciar Spark con la conexión a MongoDB
spark = SparkSession.builder \
    .appName("Autocine Big Data") \
    .master("local[*]") \
    .config("spark.mongodb.read.connection.uri", URL_MONGO) \
    .getOrCreate()

print("Spark iniciado correctamente")

# 3. Leer directamente las colecciones de MongoDB
clientes = spark.read.format("mongodb") \
    .option("database", "autocine_bigdata") \
    .option("collection", "clientes") \
    .load()

peliculas = spark.read.format("mongodb") \
    .option("database", "autocine_bigdata") \
    .option("collection", "peliculas") \
    .load()

funciones = spark.read.format("mongodb") \
    .option("database", "autocine_bigdata") \
    .option("collection", "funciones") \
    .load()

entradas = spark.read.format("mongodb") \
    .option("database", "autocine_bigdata") \
    .option("collection", "entradas") \
    .load()

# 4. Mostrar los datos usando DataFrames
print("\n=== CLIENTES ===")
clientes.show(5, truncate=False)

print("\n=== PELICULAS ===")
peliculas.show(10, truncate=False)

print("\n=== FUNCIONES ===")
funciones.show(5, truncate=False)

print("\n=== ENTRADAS ===")
entradas.show(5, truncate=False)

# 5. Mostrar la estructura de las películas
print("\n=== ESTRUCTURA DE PELICULAS ===")
peliculas.printSchema()

# 6. Procesar las películas mediante RDD
print("\n=== RDD DE PELICULAS ===")
rdd = peliculas.rdd

for fila in rdd.take(5):
    print(fila)

# 7. Analizar las películas con Spark SQL
print("\n=== PELICULAS POR GENERO ===")
peliculas.createOrReplaceTempView("peliculas")

resultado = spark.sql("""
    SELECT genero, COUNT(*) AS cantidad
    FROM peliculas
    GROUP BY genero
    ORDER BY cantidad DESC
""")

resultado.show()

# 8. Analizar las entradas vendidas
print("\n=== ENTRADAS REGISTRADAS ===")
entradas.groupBy().count().show()

# Mostrar la estructura de las entradas
print("\n=== ESTRUCTURA DE ENTRADAS ===")
entradas.printSchema()

# 9. Calcular entradas vendidas e ingresos por función
from pyspark.sql.functions import count, sum

print("\n=== VENTAS E INGRESOS POR FUNCION ===")

entradas.groupBy("id_funcion").agg(
    count("id_entrada").alias("entradas_vendidas"),
    sum("precio").alias("ingreso_total")
).orderBy("id_funcion").show()


# 10. Cerrar Spark
spark.stop()


