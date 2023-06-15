import csv
import os
import psycopg2
import time
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
print("Connection established to PostgreSQL database")

connection.autocommit = True

cursor = connection.cursor()

#EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
#PostgreSQL automatically creates an index for each unique constraint and primary key constraint to enforce uniqueness.
#Other than that, if you want a non-unique index, you will need to create it yourself in a separate CREATE INDEX query.
#https://towardsdatascience.com/how-we-optimized-postgresql-queries-100x-ff52555eabe
#https://www.postgresql.org/docs/current/sql-createindex.html
#https://wiki.postgresql.org/wiki/Performance_Optimization
#https://sematext.com/blog/postgresql-performance-tuning/

#CREATE TABLE IF NOT EXISTS co2_cars (ID INT,Country TEXT,VFN TEXT,Mp TEXT,Mh TEXT,Man TEXT,MMS TEXT,Tan TEXT,T TEXT,Va TEXT,Ve TEXT,Mk TEXT,Cn TEXT,Ct TEXT,Cr TEXT,r INT,"m (kg)" INT,Mt INT,"Enedc (g/km)" INT,"Ewltp (g/km)" INT,"W (mm)" INT,"At1 (mm)" INT,"At2 (mm)" INT,Ft TEXT,Fm TEXT,"ec (cm3)" INT,"ep (KW)" INT,"z (Wh/km)" INT,IT TEXT,"Ernedc (g/km)" FLOAT,"Erwltp (g/km)" FLOAT,De FLOAT,Vf FLOAT,Status TEXT,year INT,"Date of registration" TEXT,"Fuel consumption" INT,"Electric range (km)" INT)

#\COPY co2_cars FROM PROGRAM 'curl https://www.loicherblot.fr/rncp_ol/data-1761404.csv' (FORMAT(CSV), DELIMITER(','), HEADER);

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

time_start=time.time()

# Use an iterator to limit RAM usage
with open('data-1761404.csv', 'r') as file:
    # Prepare a CSV reader to read through the file
    reader = csv.reader(file)
    # Ignore first line (headers)
    headers = next(reader)

    # Set batch max size
    BATCH_MAX_SIZE = 100

    # Define table schema so rows can be processed within a loop
    schema={0:'INT', 1:'TEXT', 11:'TEXT', 12:'TEXT', 13:'TEXT', 14:'TEXT', 16:'SMALLINT',
      18:'SMALLINT', 20:'SMALLINT', 21:'SMALLINT', 23:'TEXT', 26:'SMALLINT', 27:'SMALLINT', 34:'SMALLINT'}
    
    # Initialize batch's size counter
    batch_size = 0
    batch_starting_row = 0

    # Initialize query string
    query = "INSERT INTO co2_vehicles VALUES "

    # Read CSV rows, process them and insert them using multirows inserts
    for i, row in enumerate(reader):
      # Prepare data to be inserted
      #print(f"Processing row #{i}...")
      if len(row) != 38:
        print("Column count doesn't match output table definition")
        continue

      if batch_size==0:
        query += '('
      else:
        query += ', ('
      
      for idx, (j, t) in enumerate(schema.items()):
        if idx > 0:
          query += ', '
        if len(row[j]) > 0:
          if t in ('BIGINT', 'FLOAT', 'INT', 'SMALLINT'):
            query += row[j]
          elif t in ('CHAR', 'DATE', 'TEXT', 'VARCHAR'):
            query += f"'{row[j]}'"
          else:
            query += 'NULL'
        else:
          query += 'NULL'
      query += ')'

      batch_size += 1
      
      # If batch has reached its max size, executes it
      if batch_size == BATCH_MAX_SIZE:
        print(f"Running multirows insert for the previous {BATCH_MAX_SIZE} rows (#{batch_starting_row} to #{i}) : {len(query)/1024:.2f} kB...")
        cursor.execute(query)
        # Reset batch
        query = "INSERT INTO co2_vehicles VALUES "
        batch_size = 0
        batch_starting_row = i
    
    # Execute batch if it hasn't been executed in the previous loop
    if batch_size > 0 and batch_size < BATCH_MAX_SIZE:
      print(f"Running multirows insert for the previous {batch_size} rows (#{batch_starting_row} to #{i}) : {len(query)/1024:.2f} kB)...")
      cursor.execute(query)

print(f"Import has been successfully completed in {time.time()-time_start:.2f}s")
# 100,000 rows
# Batch size  | Logging   | Execution time
#         100 | Logged    |        105.10s
#         100 | Unlogged  |        105.58s
#        1024 | Logged    |         20.38s
#        1024 | Unlogged  |         20.16s
#        4096 | Logged    |         31.20s
#        4096 | Unlogged  |         30.73s

# 1,761,404 rows
# Batch size  | Logging   | Execution time
#         100 | Logged    |       1869.61s
#         100 | Unlogged  |       1870.94s
#        1024 | Logged    |        416.67s
#        1024 | Unlogged  |        355.80s
#        4096 | Logged    |        448.40s
#        4096 | Unlogged  |        456.76s
#        COPY | Logged    |         30.00s

cursor.close()
connection.close()

print("EOS")