import os

DATA_BACKEND = 'neo4j'

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

# Neo4J Configuration
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
NEO4J_URI = 'bolt://localhost'

# Hive configuration

HIVE_IP = '127.0.0.1'
HIVE_PORT = 10000
HIVE_SCHEMA = 'wine'
HIVE_USER = 'root'
HIVE_PASSWORD = 'hadoop'
HIVE_TABLE = 'wine_review'
