#!/usr/bin/env python3
"""
diagnostico_mongo.py — Verifica exactamente a qué cluster/DB escribió el runner
================================================================================
Ejecutar desde la raíz del repo:
    source .venv/bin/activate
    python diagnostico_mongo.py
"""
import os, sys
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("MONGODB_URI", "")
DB_NAME = os.getenv("MONGODB_DB", "tfm")

if not URI:
    print("❌ MONGODB_URI no está definida en el .env")
    sys.exit(1)

# Ocultar password en la impresión
safe_uri = URI
if "@" in URI:
    pre, post = URI.split("@", 1)
    safe_uri = pre.split("://")[0] + "://***:***@" + post

print(f"URI (oculta): {safe_uri}")
print(f"Database    : {DB_NAME}")
print()

try:
    from pymongo import MongoClient
except ImportError:
    print("❌ pymongo no instalado: pip install 'pymongo[srv]' --break-system-packages")
    sys.exit(1)

try:
    client = MongoClient(URI, serverSelectionTimeoutMS=8000)
    info = client.server_info()
    print(f"✅ Conexión OK — servidor: {info.get('host', 'desconocido')}")
    print(f"   Versión MongoDB: {info.get('version')}")
except Exception as e:
    print(f"❌ No se pudo conectar: {e}")
    sys.exit(1)

print()
db = client[DB_NAME]

colecciones = [
    "reports", "bayesian_traces", "bayesian_reports",
    "raw_news", "ohlcv", "news", "news_filtered",
    "macro_context", "macro_news", "watchlists",
]

print(f"{'Colección':<22} {'Documentos':>12}")
print("-" * 36)
total = 0
for col in colecciones:
    n = db[col].count_documents({})
    total += n
    estado = "✅" if n > 0 else "❌"
    print(f"{estado} {col:<20} {n:>10,}")

print("-" * 36)
print(f"{'TOTAL':<22} {total:>12,}")
print()

# Muestra una fecha de ejemplo si hay reports
if db["reports"].count_documents({}) > 0:
    sample = db["reports"].find_one({}, {"report_date": 1, "tickers": 1})
    print(f"Ejemplo report: {sample.get('report_date')} — tickers: {sample.get('tickers', [])}")

if db["bayesian_traces"].count_documents({}) > 0:
    sample = db["bayesian_traces"].find_one({}, {"batch_date": 1})
    print(f"Ejemplo trace:  {sample.get('batch_date')}")

# Muestra todos los databases del cluster (para confirmar cuál es el correcto)
print()
print("Databases en este cluster:")
for d in client.list_database_names():
    n_docs = sum(client[d][c].count_documents({}) for c in client[d].list_collection_names())
    print(f"  {d:<20} ({n_docs:,} docs totales)")

client.close()
