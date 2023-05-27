"""
###################
# Cassandra setup #
###################

### Check if Java is installed
java -version

### Download Cassandra
wget "https://dlcdn.apache.org/cassandra/4.0.9/apache-cassandra-4.0.9-bin.tar.gz"

### Create Cassandra's default directories
sudo mkdir /var/lib/cassandra/
sudo mkdir /var/lib/cassandra/commitlog
sudo mkdir /var/log/cassandra/
sudo chown -R your_username /var/lib/cassandra/ /var/log/cassandra/

### Start Cassandra
apache-cassandra-4.0.9/bin/cassandra -f

# Launch CQLSH
python apache-cassandra-4.0.9/bin/cqlsh.py

### Update conf/cassandra.yaml
# Log WARN on any multiple-partition batch size exceeding this value. 5kb per batch by default.
# Caution should be taken on increasing the size of this threshold as it can lead to node instability.
batch_size_warn_threshold_in_kb: 1024

# Fail any multiple-partition batch exceeding this value. 50kb (10x warn threshold) by default.
batch_size_fail_threshold_in_kb: 10240

####################
# Python libraries #
####################
pip install --upgrade pip
pip install cassandra-driver
pip install pandas
"""
import cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

import pandas as pd
import csv

print (cassandra.__version__)

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Get Cassandra release version to check connection to the cluster
row = session.execute("SELECT release_version FROM system.local").one()
if row:
  print("Cassandra release version : "+row[0])
else:
  print("An error occurred.")

# Create co2_cars table
session.execute("USE rncp_ol")
session.execute("""CREATE TABLE IF NOT EXISTS co2_cars
  (
    ID INT,
    Country TEXT,
    VFN TEXT,
    Mp TEXT,
    Mh TEXT,
    Man TEXT,
    MMS TEXT,
    Tan TEXT,
    T TEXT,
    Va TEXT,
    Ve TEXT,
    Mk TEXT,
    Cn TEXT,
    Ct TEXT,
    Cr TEXT,
    R SMALLINT,
    "M (kg)" SMALLINT,
    Mt SMALLINT,
    "Enedc (g/km)" SMALLINT,
    "Ewltp (g/km)" SMALLINT,
    "W (mm)" SMALLINT,
    "At1 (mm)" SMALLINT,
    "At2 (mm)" SMALLINT,
    Ft TEXT,
    Fm TEXT,
    "Ec (cm3)" SMALLINT,
    "Ep (KW)" SMALLINT,
    "Z (Wh/km)" SMALLINT,
    IT TEXT,
    "Ernedc (g/km)" FLOAT,
    "Erwltp (g/km)" FLOAT,
    De FLOAT,
    Vf SMALLINT,
    Status TEXT,
    Year SMALLINT,
    "Date of registration" DATE,
    "Fuel consumption" SMALLINT,
    "Electric range (km)" SMALLINT,
    PRIMARY KEY ((Status, Year, Country, Ft), ID)
  );""")

# Use an iterator to limit RAM usage
with open('data-1023.csv', 'r') as file:
    # Prepare a CSV reader to read through the file
    reader = csv.reader(file)
    # Ignore first line (headers)
    headers = next(reader)

    # Initialize Cassandra's batch processor
    # Cassandra has a limit on the size of batch statements, which is currently set to a maximum of 65535 statements and 640kB
    batch = BatchStatement()
    # Set batch max size (in kB)
    batch_max_size = 10*1024-1

    # Define table schema so rows can be processed within a loop
    schema=('INT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT',
      'INT','INT','INT','INT','INT','INT','INT','INT','TEXT','TEXT','INT','INT','INT','TEXT','FLOAT','FLOAT','FLOAT',
      'INT','TEXT','INT','TEXT','INT','INT')
    
    # Initialize batch's size counter
    batch_size = 0

    # Read CSV rows, processes them and insert them using batch processing
    for i, row in enumerate(reader):
      # Prepare data to be inserted
      print(f"Processing row #{i}...")
      if len(row)!=38:
        print("Column count doesn't match output table definition")
        continue
      # Transform row's values based on their type (defined in schema)
      for j, t in enumerate(schema):
        if t=='INT':
          if len(row[j])>0:
            row[j]=int(row[j])
          else:
            row[j]='NULL'
        elif t=='FLOAT':
          if len(row[j])>0:
            row[j]=float(row[j])
          else:
            row[j]='NULL'
        elif t=='TEXT' or t=='DATE':
          if len(row[j])>0:
            row[j]=f"'{row[j]}'"
          else:
            row[j]='NULL'
      # Build the insert query
      query="INSERT INTO co2_cars (ID, Country, VFN, Mp, Mh, Man, MMS, Tan, T, Va, Ve, Mk, Cn, Ct, Cr, R, \"M (kg)\", Mt, \"Enedc (g/km)\", \"Ewltp (g/km)\", \"W (mm)\", \"At1 (mm)\", \"At2 (mm)\", Ft, Fm, \"Ec (cm3)\", \"Ep (KW)\", \"Z (Wh/km)\", IT, \"Ernedc (g/km)\", \"Erwltp (g/km)\", De, Vf, Status, Year, \"Date of registration\", \"Fuel consumption\", \"Electric range (km)\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      # Replace query's '?' with corresponding values (doing so with %d, %f and %s or ? and passing a list of arguments didn't work)
      query2, x = "", 0
      for c in query:
        if c=='?':
          query2+=str(row[x])
          x+=1
        else:
          query2+=c
      # If batch would exceed max_batch_size when including current query, executes it
      if (batch_size+len(query2))/1024 >= batch_max_size:
        print(f"Running batch inserts (batch size : {batch_size/1024:.1f}kB)...")
        session.execute(batch)
        # Reset batch
        batch = BatchStatement()
        batch_size = 0
      # Add query to the batch processor
      batch.add(query2)
      batch_size += len(query2)
    # Execute batch if it hasn't been executed in the previous loop
    print(f"Running batch inserts (batch size : {batch_size/1024:.1f}kB)...")
    session.execute(batch)

df=pd.DataFrame(session.execute("SELECT * FROM co2_cars"))
df.head()