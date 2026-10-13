from pymongo import MongoClient
from urllib.parse import quote_plus
import json

usuario = "difertota_db_user"
password = "AutoCine2026Fernando123"

password_codificada = quote_plus(password)

URL_MONGO = f"mongodb+srv://{usuario}:{password_codificada}@cluster0.jprrr6g.mongodb.net/"

cliente = MongoClient(URL_MONGO)
db = cliente["autocine_bigdata"]

colecciones = [
    "clientes",
    "peliculas",
    "funciones",
    "entradas"
]

for nombre in colecciones:

    datos = list(db[nombre].find())

    with open(f"{nombre}.json", "w", encoding="utf-8") as archivo:

        for documento in datos:

            documento["_id"] = str(documento["_id"])

            archivo.write(
                json.dumps(documento, ensure_ascii=False) + "\n"
            )

    print(f"{nombre}.json creado: {len(datos)} registros")

print("Exportación terminada")