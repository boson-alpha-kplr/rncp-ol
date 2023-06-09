import csv
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

host = os.environ.get('PG_HOST')
port = os.environ.get('PG_PORT')
database = os.environ.get('PG_DATABASE')
username = os.environ.get('PG_USERNAME')
password = os.environ.get('PG_PASSWORD')
if os.environ.get('PG_TIMEOUT') != None:
    timeout = os.environ.get('PG_TIMEOUT')
else:
    timeout=10

if not all([host, port, database, username, password]):
    raise Exception("Please define all required environment variables.")

connection = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=username,
    password=password,
    connect_timeout=timeout
)

cursor = connection.cursor()

cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")

results = cursor.fetchall()
for row in results:
  print(row)

cursor.execute("""CREATE TABLE IF NOT EXISTS co2_vehicles
  (
    ID INT,
    Country TEXT,
    Mk TEXT,
    Cn TEXT,
    Ct TEXT,
    Cr TEXT,
    "M (kg)" SMALLINT,
    "Enedc (g/km)" SMALLINT,
    "W (mm)" SMALLINT,
    "At1 (mm)" SMALLINT,
    Ft TEXT,
    "Ep (KW)" SMALLINT,
    "Z (Wh/km)" SMALLINT,
    Year SMALLINT
    PRIMARY KEY ((Status, Year, Country, Ft), ID)
  );""")

cursor.close()
connection.close()