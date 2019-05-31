import cassandra as cs
from cassandra.cqlengine import columns, models, connection
from cassandra.cqlengine.management import sync_table

class wine_review(models.Model):
    id = columns.Integer(primary_key=True)
    country = columns.Text()
    description = columns.Text()
    designation = columns.Text()
    points = columns.Integer()
    price = columns.Decimal()
    province = columns.Text()
    region_1 = columns.Text()
    region_2 = columns.Text()
    taster_name = columns.Text()
    taster_twitter_handle = columns.Text()
    title = columns.Text()
    variety = columns.Text()
    winery = columns.Text()

def init_app(app):
    global session
    global max_page

    cluster = cs.cluster.Cluster([app.config['CASSANDRA_IP']],port=app.config["CASSANDRA_PORT"])
    session = cluster.connect(app.config["CASSANDRA_KEYSPACE"])
    connection.setup([app.config['CASSANDRA_IP']],app.config["CASSANDRA_KEYSPACE"],protocol_version=3)
    sync_table(wine_review)
    max_page = wine_review.objects().all().count()

def list_reviews(limit=10, cursor=0, list=0):
    cursor = 0 if not cursor else int(cursor.decode())
    cursor = 0 if cursor<0 else cursor
    cursor = max_page - limit if cursor>=max_page else cursor 
    limit = int(limit)

    request = wine_review.objects().limit(cursor + limit)
    reviews = [item for item in request]
    reviews = reviews[cursor:cursor+limit+1]

    next_page = cursor + limit if len(reviews) == limit else None 

    previous_page = cursor - limit if cursor > 0 else -1

    return (reviews ,next_page,previous_page)

def text_search(query, limit=10, cursor=None, list=0):
    cursor = 0 if not cursor else int(cursor.decode())
    cursor = 0 if cursor<0 else cursor
    cursor = max_page - limit if cursor>=max_page else cursor 
    limit = int(limit)

    request = wine_review.objects.filter(designation__like='%'+query+"%").limit(cursor+limit).allow_filtering()
    reviews = [item for item in request]
    reviews = reviews[cursor:cursor+limit+1]

    next_page = cursor + limit if len(reviews) == limit else None 

    previous_page = cursor - limit if cursor > 0 else -1


    return (reviews, next_page, previous_page)

def custom_search(query, limit=10, cursor=None, list=0):
    
    cursor = 0 if not cursor else int(cursor.decode())
    cursor = 0 if cursor<0 else cursor
    cursor = max_page - limit if cursor>=max_page else cursor 
    limit = int(limit)


    if (';' in query): # Several search values
        categories = query.split(';')
    else:
        categories = [query]
    q = "SELECT * FROM wine_review WHERE "

    #  Traverse all query categories
    for cat in categories:
        # Separate key from value(s)
        [key, value] = cat.split(':')

         


        if (key == 'price' or key == 'points'):
            # Numerical value
            if ('<=' in value):
                num = value.split('<=')[-1]
                q += key+'<='+num
                #match["$match"][key] = {'$lte' : float(num)}
            elif ('>=' in value):
                num = value.split('>=')[-1]
                q += key+'>='+num
                #match["$match"][key] = {'$gte' : float(num)}
            elif ('<' in value):
                num = value.split('<')[-1]
                q += key+'<'+num
                #match["$match"][key] = {'$lt' : float(num)}
            elif ('>' in value):
                num = value.split('>')[-1]
                q += key+'>'+num
                #match["$match"][key] = {'$gt' : float(num)}
            else:
                q += key+'<='+num
                #match["$match"][key] = {'$eq' : float(value)}

        else:
            # Literal value
            if (',' in value): # List of values
                values_l = value.split(',')
                q += key +" in "+values_l
                #q.filter(key=num)
                #match["$match"][key] = {"$in" : values_l}
            else: # Only one value
                q += key +"='"+value+"'"
                # match["$match"][key] = value

        q += ' and '

    q = q[:-4]
    q += " limit "+str((cursor + limit))+';'
    print(q)
    request = session.execute(q)
    reviews = [item for item in request]
    reviews = reviews[cursor:cursor+limit+1]

    next_page = cursor + limit if len(reviews) == limit else None 

    previous_page = cursor - limit if cursor > 0 else -1


    
    
    
    
    return (reviews, next_page, previous_page)