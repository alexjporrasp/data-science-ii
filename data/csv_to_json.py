from pymongo import MongoClient
import sys, csv, json

if __name__=='__main__':
    
    if (len(sys.argv) != 3):
        print('wrong number or arguments. You must specify a csv file and an output name for the json file.')
        exit()

    csv_path = sys.argv[1]
    json_name = sys.argv[2]

    with open(csv_path) as csv_file: 
        json_file = open(json_name, 'w+')
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        header = None
        for row in csv_reader:
            print("line #", line_count)
            if line_count == 0:
                header = row
            else:
                review = dict()
                for i, col in enumerate(row):
                    if col == "":
                        continue

                    if header[i] == "_id":
                        review[header[i]] = int(col)
                    else :
                        try:
                            review[header[i]] = float(col)
                        except ValueError:
                            review[header[i]] = col
                        
                json.dump(review, json_file)
                json_file.write('\n')
            line_count += 1
        print(f'Inserted {line_count} documents.')




