from flask_pymongo import PyMongo

builtin_list = list 

mongo = None

def _id(id):
    if not isinstance(id, ObjectId):
        return ObjectId(id)
    return id

def from_mongo(data):
    """
    Translates the MongoDB dictionary format into the format that's expected
    by the application.
    """
    if not data:
        return None

    data['id'] = str(data['_id'])
    return data

def init_app(app):
    global mongo 

    mongo = PyMongo(app)
    mongo.init_app(app)

def list_reviews(limit=10, cursor=None, list=0):
    cursor = int(cursor) if cursor else 0

    results = mongo.db.wine_reviews.find(skip=cursor, limit=10)

    reviews = builtin_list(map(from_mongo, results))

    next_page = cursor + limit if len(reviews) == limit else None 

    return (reviews, next_page)