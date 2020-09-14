import pymongo

from environment import MONGO_NAME, MONGO_HOST

my_client = pymongo.MongoClient(MONGO_HOST)
my_db = my_client[MONGO_NAME]


def push(collection_name, parsed_raw_dic):
    if len(parsed_raw_dic) == 0:
        return
    my_collection = my_db[collection_name]
    my_collection.insert_many(parsed_raw_dic)


def get(collection_name, fields_dic):
    db_collection = my_db[collection_name]
    result = []
    cursor = db_collection.find({}, fields_dic)
    for row in cursor:
        del row['_id']
        result.append(row)
    return result


def check_connection():
    print(my_client.ranbase)


if __name__ == '__main__':
    result = get('ZTE900BssFunction', {'userLabel':1})
    print(result)