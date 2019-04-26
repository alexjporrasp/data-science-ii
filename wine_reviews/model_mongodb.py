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

    previous_page = cursor - limit if cursor > 0 else -1 

    return (reviews, next_page, previous_page)

'''
db.wine_reviews.aggregate(
    [
        {
            $match: {
                "country" : { $in : ["Italy"]}
            }
        }
    ]
);

'''

def text_search(query, limit=10, cursor=None, list=0):
    cursor = int(cursor) if cursor else 0

    results = mongo.db.wine_reviews.find({
        "$text": {
            "$search": query
        }
    }, skip=cursor, limit=limit)

    reviews = builtin_list(map(from_mongo, results))
    
    next_page = cursor + limit if len(reviews) == limit else None 

    previous_page = cursor - limit if cursor > 0 else -1 

    return (reviews, next_page, previous_page)



def custom_search(query, limit=10, cursor=None, list=0):
    cursor = int(cursor) if cursor else 0

    pipeline = []

    if (';' in query): # Several search values
        categories = query.split(';')
    else:
        categories = [query]

    #  Traverse all query categories
    for cat in categories:
        # Separate key from value(s)
        [key, value] = cat.split(':')

        match = {
            "$match" : {

            }
        }

        if (key == 'price' or key == 'points'):
            # Numerical value
            if ('<=' in value):
                num = value.split('<=')[-1]
                match["$match"][key] = {'$lte' : float(num)}
            elif ('>=' in value):
                num = value.split('>=')[-1]
                match["$match"][key] = {'$gte' : float(num)}
            elif ('<' in value):
                num = value.split('<')[-1]
                match["$match"][key] = {'$lt' : float(num)}
            elif ('>' in value):
                num = value.split('>')[-1]
                match["$match"][key] = {'$gt' : float(num)}
            else:
                match["$match"][key] = {'$eq' : float(value)}

        else:
            # Literal value
            if (',' in value): # List of values
                values_l = value.split(',')
                match["$match"][key] = {"$in" : values_l}
            else: # Only one value
                match["$match"][key] = value

        pipeline.append(match)


    pipeline.append({"$skip" : cursor})
    pipeline.append({"$limit" : limit})

    print(pipeline)

    results = mongo.db.wine_reviews.aggregate(pipeline)

    reviews = builtin_list(map(from_mongo, results))
    
    next_page = cursor + limit if len(reviews) == limit else None 

    previous_page = cursor - limit if cursor > 0 else -1 

    return (reviews, next_page, previous_page)



    
