import wine_reviews, config

app = wine_reviews.create_app(config)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port='8090', debug=True)