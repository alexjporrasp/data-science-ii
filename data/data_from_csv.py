from pymongo import MongoClient
import sys, csv

if __name__=='__main__':
    
    if (len(sys.argv) != 2):
        print('wrong number or arguments. you must specify a csv file.')
        exit()

    csv_path = sys.argv[1]
    
    client = MongoClient() # Local host
    db = client.test
    wine_reviews = db.wine_reviews


    with open(csv_path) as csv_file: 
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        header = None
        for row in csv_reader:
            if line_count == 0:
                header = row
            else:
                review = {}
                for i, col in enumerate(row):
                    review[i] = col[i]
                wine_reviews.insert(review)
            line_count += 1
        print(f'Inserted {line_count} documents.')




