from pyspark.sql import SparkSession
from urllib.parse import quote_plus
from pyspark.sql.functions import count, sum

# 1. Configurar la conexion con MongoDB Atlas
usuario = "difertota_db_user"
password = quote_plus("AutoCine2026Fernando123")

URL_MONGO = (
    f"mongodb+srv://{usuario}:{password}"
    "@cluster0.jprrr6g.mongodb.net/autocine_bigdata"
)

# 2. Iniciar Spark
spark = SparkSession.builder \
    .appName("Autocine Big Data") \
    .config("spark.mongodb.read.connection.uri", URL_MONGO) \
    .getOrCreate()

print("Spark iniciado correctamente")

# 3. Leer las colecciones de MongoDB
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

print("\n=== ESTRUCTURA DE ENTRADAS ===")
entradas.printSchema()

# 6. Procesar datos usando RDD
print("\n=== PROCESAMIENTO CON RDD ===")
rdd_peliculas = peliculas.rdd

for pelicula in rdd_peliculas.take(5):
    print(pelicula)

# 7. Analizar peliculas con Spark SQL
print("\n=== PELICULAS POR GENERO ===")
peliculas.createOrReplaceTempView("peliculas")

resultado = spark.sql("""
    SELECT genero, COUNT(*) AS cantidad
    FROM peliculas
    GROUP BY genero
    ORDER BY cantidad DESC
""")

resultado.show()

# 8. Calcular ventas e ingresos por funcion
print("\n=== VENTAS E INGRESOS POR FUNCION ===")

entradas.groupBy("id_funcion").agg(
    count("id_entrada").alias("entradas_vendidas"),
    sum("precio").alias("ingreso_total")
).orderBy("id_funcion").show()

# 9. Relacionar funciones con peliculas
# Estos IDs deben coincidir con los datos originales.
lista_peliculas = [
    (1, "Jurassic World"),
    (2, "El Conjuro"),
    (3, "Super Mario Bros"),
    (4, "Avengers: Endgame"),
    (5, "Coco"),
    (6, "Interstellar"),
    (7, "Toy Story 4"),
    (8, "Spider-Man: No Way Home"),
    (9, "Batman"),
    (10, "Joker")
]

peliculas_con_id = spark.createDataFrame(
    lista_peliculas,
    ["id_pelicula", "pelicula"]
)

print("\n=== FUNCIONES CON SUS PELICULAS ===")

funciones.join(
    peliculas_con_id,
    "id_pelicula",
    "left"
).select(
    "id_funcion",
    "id_pelicula",
    "pelicula",
    "fecha",
    "hora"
).orderBy("id_funcion").show(truncate=False)

# 10. Generar el reporte completo de ventas
# Los IDs de clientes son una asociacion manual.
lista_clientes = [
    (1, "Fernando Velasquez"),
    (2, "Daniela Silva"),
    (3, "Valeria Rojas"),
    (4, "Maria Lopez"),
    (5, "Andrea Torres"),
    (6, "Jorge Perez"),
    (7, "Carlos Ramirez"),
    (8, "Luis Mendoza"),
    (9, "Sofia Castro"),
    (10, "Diego Flores")
]

clientes_con_id = spark.createDataFrame(
    lista_clientes,
    ["id_cliente", "cliente"]
)

reporte_ventas = (
    entradas
    .join(clientes_con_id, "id_cliente", "left")
    .join(
        funciones.select(
            "id_funcion",
            "id_pelicula",
            "fecha",
            "hora"
        ),
        "id_funcion",
        "left"
    )
    .join(peliculas_con_id, "id_pelicula", "left")
    .select(
        "id_entrada",
        "cliente",
        "pelicula",
        "fecha",
        "hora",
        "precio"
    )
    .orderBy("id_entrada")
)

print("\n=== REPORTE COMPLETO DE VENTAS ===")
reporte_ventas.show(20, truncate=False)

# 11. Cerrar Spark
spark.stop()
print("Procesamiento finalizado")
