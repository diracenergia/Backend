import os
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Falta la env DATABASE_URL")

def get_conn():
    # Pool simple por conexión a demanda (suficiente para un backend chico)
    # sslmode=require ya viene en tu cadena de Supabase.
    return psycopg.connect(DATABASE_URL, connect_timeout=10, row_factory=dict_row)
