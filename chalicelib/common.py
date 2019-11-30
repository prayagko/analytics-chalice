from chalicelib import db
import arrow

mongo = db.mongo


def get_institution_dict():
    institutions = map(lambda x: x, mongo.institutions.find())
    institution_dict = {}
    for institution in institutions:
        institution_dict[institution['name']] = institution['shortid']
    return institution_dict


def get_oid_from_shortid(shortid):
    oid = mongo.institutions.find_one({'shortid': shortid}, {'_id': 1}).get('_id')
    return oid

def get_date_interval(start_date_string, end_date_string):
    start_date = arrow.get(start_date_string, 'YYYY-MM-DD').replace(tzinfo='Asia/Kathmandu')
    start_date_utc = start_date.to('utc')
    end_date = arrow.get(end_date_string, 'YYYY-MM-DD').replace(tzinfo='Asia/Kathmandu').shift(days=+1)
    end_date_utc = end_date.to('utc')
    return start_date_utc.datetime, end_date_utc.datetime

# number of linked guardians by institution
def get_number_of_linked_guardians(oid):
    count = mongo.users.count_documents({'institution':oid, 'role': 'guardians'})
    return count

# this function gets the "user" id of linked users
def get_user_id(oid, user_type):
    if user_type == 'teacher':
        user = mongo.teachers.find_one({'_id': oid})
    elif user_type == 'guardian':
        user = mongo.guardians.find_one({'_id': oid})
    if 'linking' in user:
        user_id = user['linking'].get('auser')
    else:
        user_id = None
    return user_id


def get_document_count(collection):
    count = mongo[collection].count_documents({})
    return count


def check_documents_exist(collection, key, values):
    check = True if mongo[collection].count_documents({key: {"$in": values}}) > (len(values)-1) else False
    return check