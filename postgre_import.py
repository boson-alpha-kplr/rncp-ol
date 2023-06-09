import csv
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

host = os.environ.get('PG_HOST')
if host == None:
  raise Exception("Please define PG_HOST environment variable in .env file.")
port = os.environ.get('PG_PORT')
if host == None:
  raise Exception("Please define PG_PORT environment variable in .env file.")
database = os.environ.get('PG_DATABASE')
if host == None:
  raise Exception("Please define PG_DATABASE environment variable in .env file.")
username = os.environ.get('PG_USERNAME')
if host == None:
  raise Exception("Please define PG_USERNAME environment variable in .env file.")
password = os.environ.get('PG_PASSWORD')
if host == None:
  raise Exception("Please define PG_PASSWORD environment variable in .env file.")
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

#EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
#PostgreSQL automatically creates an index for each unique constraint and primary key constraint to enforce uniqueness.
#Other than that, if you want a non-unique index, you will need to create it yourself in a separate CREATE INDEX query.
#https://towardsdatascience.com/how-we-optimized-postgresql-queries-100x-ff52555eabe
#https://www.postgresql.org/docs/current/sql-createindex.html
#https://wiki.postgresql.org/wiki/Performance_Optimization
#https://sematext.com/blog/postgresql-performance-tuning/

#CREATE TABLE IF NOT EXISTS co2_cars (ID INT,Country TEXT,VFN TEXT,Mp TEXT,Mh TEXT,Man TEXT,MMS TEXT,Tan TEXT,T TEXT,Va TEXT,Ve TEXT,Mk TEXT,Cn TEXT,Ct TEXT,Cr TEXT,r INT,"m (kg)" INT,Mt INT,"Enedc (g/km)" INT,"Ewltp (g/km)" INT,"W (mm)" INT,"At1 (mm)" INT,"At2 (mm)" INT,Ft TEXT,Fm TEXT,"ec (cm3)" INT,"ep (KW)" INT,"z (Wh/km)" INT,IT TEXT,"Ernedc (g/km)" FLOAT,"Erwltp (g/km)" FLOAT,De FLOAT,Vf FLOAT,Status TEXT,year INT,"Date of registration" TEXT,"Fuel consumption" INT,"Electric range (km)" INT)

#\COPY co2_cars FROM PROGRAM 'curl https://www.loicherblot.fr/rncp_ol/data-100000.csv' (FORMAT(CSV), DELIMITER(','), HEADER);

#https://towardsdatascience.com/upload-your-pandas-dataframe-to-your-database-10x-faster-eb6dc6609ddf

cursor.execute("""CREATE TABLE IF NOT EXISTS co2_vehicles
  (
    ID INT NOT NULL,
    Country TEXT NOT NULL,
    Mk TEXT NOT NULL,
    Cn TEXT,
    Ct TEXT,
    Cr TEXT,
    M SMALLINT,
    Enedc SMALLINT,
    W SMALLINT,
    At1 SMALLINT,
    Ft TEXT NOT NULL,
    Ep SMALLINT,
    Z SMALLINT,
    Year SMALLINT NOT NULL,
    CONSTRAINT primary_key PRIMARY KEY(Year, Country, Ft, ID)
  ) PARTITION BY LIST (Ft);""")

cursor.execute("""CREATE TABLE IF NOT EXISTS co2_vehicles_electric PARTITION OF co2_vehicles
  FOR VALUES IN ('diesel/electric', 'Diesel-Electric', 'Electric', 'HYBRID/PETROL/E',
  'PETROL PHEV', 'PETROL/ELECTRIC', 'Petrol-Electric');""")

cursor.execute("CREATE TABLE IF NOT EXISTS co2_vehicles_thermal PARTITION OF co2_vehicles DEFAULT;")

cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")

results = cursor.fetchall()
for row in results:
  print(row[0])

cursor.close()
connection.close()

print("EOS")