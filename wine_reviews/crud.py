import os, re
from wine_reviews import get_model
from flask import Flask, Blueprint, redirect, render_template, request, url_for, flash, session

crud = Blueprint('crud', __name__)

@crud.route("/", methods=['GET'])
def list():
    token = request.args.get('page_token', None)
    if token:
        token = token.encode('utf-8')
    
    search_query = request.args.get('search')

    if (request.method == 'GET' and 
        search_query is not None ):
        print(search_query)

        # Find out whether it is a custom search or text search
        if (':' in search_query): 
            # It is a custom search
            reviews, next_page_token, previous_page_token = get_model().custom_search(request.args.get('search'), cursor=token)
        else:
            # Text query
            reviews, next_page_token, previous_page_token = get_model().text_search(request.args.get('search'), cursor=token)
    else:            
        reviews, next_page_token, previous_page_token = get_model().list_reviews(cursor=token)

    print(previous_page_token)
    return render_template(
        "list.html",
        reviews=reviews,
        next_page_token=next_page_token,
        previous_page_token=previous_page_token
    )

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