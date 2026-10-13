from pymongo import MongoClient
from urllib.parse import quote_plus

usuario = "difertota_db_user"
password = "AutoCine2026Fernando123"

password_codificada = quote_plus(password)

URL_MONGO = f"mongodb+srv://{usuario}:{password_codificada}@cluster0.jprrr6g.mongodb.net/"

cliente = MongoClient(URL_MONGO)

db = cliente["autocine_bigdata"]

colecciones = ["clientes", "peliculas", "funciones", "entradas"]

for nombre in colecciones:

    print("\n==============================")
    print("COLECCIÓN:", nombre)
    print("==============================")

    datos = db[nombre].find().limit(3)

    for documento in datos:
        print(documento)