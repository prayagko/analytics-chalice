from chalicelib import db

mongo = db.mongo

# user here refers to guardians and teachers whether they are linked in app or not
# gets all users from institution using institution oid
def get_users_list(oid, user_type):
    if user_type == 'teacher':
        user = mongo.teachers.find({'institution': oid}, {'createdAt': 0, '__v': 0})
    if user_type == 'guardian':
        user = mongo.guardians.find({'institution': oid}, {'createdAt': 0, '__v': 0})
    return list(user)




def get_activity_id(activity):
    oid = mongo.tags.find_one({'name': activity}, {'_id': 1}).get('_id')
    return oid


# get teacher report for any activity or moment in an institution
def get_activity_report(activity_list, interval, oid):
    teacher_list = get_users_list(oid, user_type='teacher')
    report = []
    for teacher in teacher_list:
        teacher_report = {'shortid': teacher['shortid'], 'name': teacher['name']}
        analytics = {}
        for activity in activity_list:
            if activity == 'moment':
                if 'linking' in teacher:
                    user_id = teacher['linking'].get('user')
                else:
                    user_id = None
                count = mongo.moments.count_documents(
                    {'createdAt': {'$gte': interval[0], '$lt': interval[1]},
                     'createdBy': user_id})
                analytics[activity]={'total_count': count}
            else:
                activity_id = get_activity_id(activity)
                count = mongo.activities.count_documents(
                    {'createdAt': {'$gte': interval[0], '$lt': interval[1]},
                     'createdBy': teacher['_id'], 'type': activity_id})
                analytics[activity] = {'total_count': count}
            teacher_report['analytics'] = analytics
        report.append(teacher_report)
    return report


