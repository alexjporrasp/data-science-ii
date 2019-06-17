from py2neo import Graph

graph = None

builtin_list = list

def init_app(app):
    global graph

    graph = Graph(app.config['NEO4J_URI'], username = app.config['NEO4J_USER'], password = app.config['NEO4J_PASSWORD'])

def list_reviews(limit=10, cursor=None, list=0):
    cursor = int(cursor) if cursor else 0
    
    query = '''
    MATCH (w:Wine)<-[:DESCRIBES]-(r:Review) 
    RETURN w.title AS title, r.description AS description
    SKIP {cursor}
    LIMIT {limit}
    '''

    reviews = graph.run(query, cursor=cursor, limit=limit).data()

    next_page = cursor + limit if len(reviews) == limit else None

    previous_page = cursor - limit if cursor > 0 else -1

    return (reviews, next_page, previous_page)


def text_search(query, limit=10, cursor=None, list=0):
    cursor = int(cursor) if cursor else 0

    # CALL db.index.fulltext.queryNodes("titlesAndDescriptions", "Full Metal Jacket") YIELD node, score
    # RETURN node.title, node.review, score

    _query = '''
    CALL db.index.fulltext.queryNodes("titlesAndDescriptions", '{}') 
    YIELD node, score 
    RETURN node.title AS title, node.description AS description, score 
    SKIP {}
    LIMIT {}
    '''.format(query, cursor, limit)
    
    results = graph.run(_query).data()

    _query = "MATCH (w:Wine)<-[:DESCRIBES]-(r:Review) WHERE "
    
    i = 0

    for node in results:
        if i > 0:
            _query += "OR "

        if node['description'] is None:
            _query += "w.title = \"" + node['title'] + "\" "
        
        if node['title'] is None:
            _query += "r.description = \"" + node['description'] + "\" "

        i += 1

    _query += "RETURN w.title AS title, r.description AS description SKIP {} LIMIT {}".format(cursor, limit)

    reviews = graph.run(_query).data()

    next_page = cursor + limit if len(reviews) == limit else None 

    previous_page = cursor - limit if cursor > 0 else -1

    return (reviews, next_page, previous_page)
    

def custom_search(query, limit=10, cursor=None, list=0):
    cursor = int(cursor) if cursor else 0

    trans = {
        'description': 'rv.description',
        'points': 'rv.points',
        'title': 'w.title',
        'price': 'w.price',
        'taster_name' : 't.name',
        'taster_twitter_handle' : 't.twitter_handle',
        'region_1' : 'r.name',
        'province' : 'p.name',
        'country' : 'c.name',
        'designation' : 'v.designation',
        'variety' : 'g.variety',
        'winery' : 'wy.name'
    }

    _query = '''
    MATCH (rv:Review)
    MATCH (w:Wine)
    MATCH (t:Taster)
    MATCH (r:Region)
    MATCH (p:Province)
    MATCH (c:Country)
    MATCH (v:Vineyard)
    MATCH (g:Grapes)
    MATCH (wy:Winery)
    WITH r,p,c,t,rv,w,v,g,wy
    MATCH (r)-[:IS_IN]->(p)
    MATCH (p)-[:IS_IN]->(c)
    MATCH (t)-[:WROTE]->(rv)-[:DESCRIBES]->(w)
    MATCH (w)-[:MADE_WITH]->(g)-[:CAME_FROM]->(v)
    MATCH (wy)-[:CREATED]->(w)
    MATCH (w)-[:CAME_FROM]->(r)
    '''

    if (';' in query):
        categories = query.split(';')
    else:
        categories = [query]

    i = 0
    for cat in categories:
        [key, value] = cat.split(':')

        if i > 0:
            _query += "AND "
        else:
            _query += "WHERE "


        if (key == 'points' or key == 'price'):
            # Numerical value
            if ('<=' in value):
                num = value.split('<=')[-1]
                _query += "{} <= {} ".format(trans[key], num)
            elif ('>=' in value):
                num = value.split('>=')[-1]
                _query += "{} >= {} ".format(trans[key], num)
            elif ('<' in value):
                num = value.split('<')[-1]
                _query += "{} < {} ".format(trans[key], num)
            elif ('>' in value):
                num = value.split('>')[-1]
                _query += "{} > {} ".format(trans[key], num)
            else:
                _query += "{} = {} ".format(trans[key], num)
        else:
            # Literal value
            if(',' in value):
                values_l = value.split(',')
                quoted_values = builtin_list(map(lambda z: "\"" + str(z) + "\"", values_l))
                values_s = ','.join(quoted_values)
                values_s = "[" + values_s + "] "
                _query += "{} IN {}".format(trans[key], values_s)
                
            else:
                _query += "{} = \"{}\" ".format(trans[key], value)

        i += 1

    _query += "RETURN DISTINCT w.title AS title, rv.description AS description SKIP {} LIMIT {}".format(cursor, limit)
    
    print(_query)
    reviews = graph.run(_query).data()

    next_page = cursor + limit if len(reviews) == limit else None 

    previous_page = cursor - limit if cursor > 0 else -1

    return (reviews, next_page, previous_page)


# CALL db.index.fulltext.createNodeIndex("titlesAndDescriptions",["Wine", "Review"],["title", "description"])