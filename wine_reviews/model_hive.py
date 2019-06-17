from pyhive import hive

global mode
global query
global search

search = ""
mode = -1
query = {}

def to_dict(records):
    ans = list()
    container = {} 
    for record in records:
        #print(record)
        container['country'] = record[0]
        container['description'] = record[1]
        container['designation'] = record[2]
        container['points'] = record[3]
        container['price'] = record[4]
        container['province'] = record[5]
        container['region_1'] = record[6]
        container['region_2'] = record[7]
        container['taster_name'] = record[8]
        container['taster_twitter_handle'] = record[9]
        container['title'] = record[10]
        container['variety'] = record[11]
        container['winery'] = record[12]
        ans = ans + [container]
        container = {}
    return ans
def init_app(app):
    global handler
    global table
    global mode
    global query
    global search

    search = ""

    conn = hive.Connection(host=app.config["HIVE_IP"], port=app.config["HIVE_PORT"], username=app.config["HIVE_USER"],auth="CUSTOM",password=app.config["HIVE_PASSWORD"],database=app.config["HIVE_SCHEMA"])
    table = app.config["HIVE_TABLE"]
    handler = conn.cursor()

def list_reviews(limit=10, cursor=0, list=0):
    global mode
    global query

    if mode != 0:
        mode = 0
        cursor = int(cursor) if cursor else 0
        handler.execute("SELECT * FROM %s" % (table))
        query = to_dict(handler.fetchmany(limit))
        reviews = query[cursor:cursor+limit]
        


        next_page = cursor + limit if len(reviews) == limit else None 

        previous_page = cursor - limit if cursor > 0 else -1 
    else:
        cursor = int(cursor) if cursor else 0

        print(len(query))

        if cursor + limit >= len(query):
            query = query + to_dict(handler.fetchmany(limit))

        reviews = query[cursor:cursor+limit]

        next_page = cursor + limit if len(reviews) == limit else None 

        previous_page = cursor - limit if cursor > 0 else -1 

    return (reviews, next_page, previous_page)

def text_search(query_user, limit=10, cursor=None, list=0):
    global mode
    global query
    global search

    if mode != 1 or query_user != search:
        search = query_user
        mode = 1 

        cursor = int(cursor) if cursor else 0

        select_query = 'SELECT * FROM {0} where title like "%{1}%" or description like "{2}" or designamtion like "{3}"'

        handler.execute(select_query.format(table,query_user,query_user,query_user,limit))

        query = to_dict(handler.fetchall())

        reviews = query[cursor:cursor+limit]
        
        next_page = cursor + limit if len(reviews) == limit else None 

        previous_page = cursor - limit if cursor > 0 else -1 
    else:
        if cursor + limit >= len(query):
            query = query + to_dict(handler.fetchmany(limit))

        reviews = query[cursor:cursor+limit]

        next_page = cursor + limit if len(reviews) == limit else None 

        previous_page = cursor - limit if cursor > 0 else -1 
    return (reviews, next_page, previous_page)

def custom_search(query_user, limit=10, cursor=0, list=0):
    global mode
    global query
    global search
    
    if mode != 2 or query_user != search:    
        search = query_user
        mode = 2 

        cursor = int(cursor) if cursor else 0

        if (';' in query_user): # Several search values
            categories = query_user.split(';')
        else:
            categories = [query_user]
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
                # Only one value
                q += key +"='"+value+"'"
                    # match["$match"][key] = value

            q += ' and '
        q = q[:-4]
        handler.execute(q)
        query = to_dict(handler.fetchmany(limit))
        reviews = query[cursor:cursor+limit]

        next_page = cursor + limit if len(reviews) == limit else None 

        previous_page = cursor - limit if cursor > 0 else -1 
    else:
        cursor = int(cursor) if cursor else 0

        if cursor + limit >= len(query):
            query = query + to_dict(handler.fetchmany(limit))

        reviews = query[cursor:cursor+limit]

        next_page = cursor + limit if len(reviews) == limit else None 

        previous_page = cursor - limit if cursor > 0 else -1 

    return (reviews, next_page, previous_page)

        