import os
from wine_reviews import get_model
from flask import Flask, Blueprint, redirect, render_template, request, url_for, flash, session

crud = Blueprint('crud', __name__)

@crud.route("/")
def list():
    token = request.args.get('page_token', None)
    if token:
        token = token.encode('utf-8')
    
    reviews, next_page_token, previous_page_token = get_model().list_reviews(cursor=token)

    print(previous_page_token)
    return render_template(
        "list.html",
        reviews=reviews,
        next_page_token=next_page_token,
        previous_page_token=previous_page_token
    )

