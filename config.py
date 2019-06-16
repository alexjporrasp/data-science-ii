import os

DATA_BACKEND = 'hive'

# mongo configuration

MONGO_USER = ''
MONGO_PASSWORD = ''

MONGO_IP = 'localhost'
MONGO_PORT = '27017'
MONGO_DB = 'test'
MONGO_URI = \
    'mongodb://'+MONGO_IP+':'+MONGO_PORT+'/'+MONGO_DB

# Cassandra configuration

CASSANDRA_IP = 'localhost'
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = 'wine_review'

# Hive configuration

HIVE_IP = '127.0.0.1'
HIVE_PORT = 10000
HIVE_SCHEMA = 'wine'
HIVE_USER = 'root'
HIVE_PASSWORD = 'hadoop'
HIVE_TABLE = 'wine_review'
