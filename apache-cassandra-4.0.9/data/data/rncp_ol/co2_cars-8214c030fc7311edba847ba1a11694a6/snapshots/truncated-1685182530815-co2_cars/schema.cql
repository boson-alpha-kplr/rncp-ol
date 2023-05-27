CREATE TABLE IF NOT EXISTS rncp_ol.co2_cars (
    status text,
    year smallint,
    country text,
    ft text,
    id int,
    "At1 (mm)" smallint,
    "At2 (mm)" smallint,
    "Date of registration" date,
    "Ec (cm3)" smallint,
    "Electric range (km)" smallint,
    "Enedc (g/km)" smallint,
    "Ep (KW)" smallint,
    "Ernedc (g/km)" float,
    "Erwltp (g/km)" float,
    "Ewltp (g/km)" smallint,
    "Fuel consumption" smallint,
    "M (kg)" smallint,
    "W (mm)" smallint,
    "Z (Wh/km)" smallint,
    cn text,
    cr text,
    ct text,
    de float,
    fm text,
    it text,
    man text,
    mh text,
    mk text,
    mms text,
    mp text,
    mt smallint,
    r smallint,
    t text,
    tan text,
    va text,
    ve text,
    vf smallint,
    vfn text,
    PRIMARY KEY ((status, year, country, ft), id)
) WITH ID = 8214c030-fc73-11ed-ba84-7ba1a11694a6
    AND CLUSTERING ORDER BY (id ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
