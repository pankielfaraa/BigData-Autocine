from pymongo import MongoClient
from urllib.parse import quote_plus

usuario = "difertota_db_user"
password = "AutoCine2026Fernando123"

# Codificar la contraseña para usarla en la URL
password_codificada = quote_plus(password)

URL_MONGO = f"mongodb+srv://{usuario}:{password_codificada}@cluster0.jprrr6g.mongodb.net/"

try:
    cliente = MongoClient(URL_MONGO)

    cliente.admin.command("ping")

    print("Conexión exitosa con MongoDB Atlas")

    db = cliente["autocine_bigdata"]

    print("\nColecciones:")

    for coleccion in db.list_collection_names():
        print("-", coleccion)

except Exception as e:
    print("Error de conexión:")
    print(e)