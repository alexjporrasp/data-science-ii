import os

DATA_BACKEND = 'mongodb'

# mongo configuration

MONGO_USER = ''
MONGO_PASSWORD = ''

MONGO_IP = 'localhost'
MONGO_PORT = '27017'
MONGO_DB = 'test'
MONGO_URI = \
    'mongodb://'+MONGO_IP+':'+MONGO_PORT+'/'+MONGO_DB