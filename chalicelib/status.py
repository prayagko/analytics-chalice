from chalicelib import activity as activity_module, db
import itertools

mongo = db.mongo


def get_number_of_students(oid):
    count = mongo.students.count_documents({'institution':oid})
    return count


def guardian_link_check(oid):
    db_guardian = mongo.guardians.find_one({'_id': oid})
    if 'linking' in db_guardian:
        issue_status = db_guardian['linking']['token']['status']
        if issue_status == 'linked':
            return 'linked'
    return 'issued'


def number_of_students_with_linked_guardians(oid):
    counter = 0
    for student in mongo.students.find({'institution': oid}):
        has_linked_guardian = False
        if len(student['guardians']):
            for guardian in student['guardians']:
                if (has_linked_guardian):
                    continue
                if guardian['guardian']:
                    guardian_oid = guardian['guardian']
                    if guardian_link_check(guardian_oid)=='linked':
                        has_linked_guardian = True
                        counter = counter + 1
    return counter


def get_activity_id(activity):
    oid = mongo.tags.find_one({'name': activity}, {'_id': 1}).get('_id')
    return oid


def get_users_list(oid, user_type):
    if user_type == 'teacher':
        user = mongo.teachers.find({'institution': oid}, {'createdAt': 0, '__v': 0})
    if user_type == 'guardian':
        user = mongo.guardians.find({'institution': oid}, {'createdAt': 0, '__v': 0})
    user_list = map(lambda x: x, user)
    return list(user_list)


def get_teacher_count(oid):
    count = mongo.teachers.count_documents({'institution': oid})
    return count


def get_activity_count(activity, interval, oid):
    if activity == 'moment':
        count = mongo.moments.count_documents(
            {'createdAt': {'$gte': interval[0], '$lt': interval[1]}, 'institution':oid})
    else:
        teacher_list = get_users_list(oid, user_type='teacher')
        activity_id = get_activity_id(activity)
        count = 0
        if teacher_list:
            for teacher in teacher_list:
                activity_count = mongo.activities.count_documents(
                    {'createdAt': {'$gte': interval[0], '$lt': interval[1]},
                     'createdBy': teacher['_id'], 'type': activity_id})
                count += activity_count
        else:
            count = 0
    return count



def get_status_metrics(oid,interval, activities):
    student_count = get_number_of_students(oid)
    students_w_one_linked = number_of_students_with_linked_guardians(oid)
    if student_count > 0:
        student_linked_percentage = (students_w_one_linked/student_count) * 100
    else:
        student_linked_percentage = 'N/A'
    active_list = activity_module.get_active_users(oid, interval)
    user_list = list(map(lambda x: x['_id'], active_list))
    guardians = activity_module.guardians_from_users(user_list)
    guardian_list = list(map(lambda x: x['_id'], guardians))
    students = activity_module.get_students_from_guardians(guardian_list)
    if students_w_one_linked > 0:
        active_guardians_percentage = (len(students)/students_w_one_linked)*100
    else:
        active_guardians_percentage = 'N/A'
    moment_count = get_activity_count('moment', interval, oid)

    total_activity = 0
    for activity in activities:
        activity_count = get_activity_count(activity, interval, oid)
        total_activity += activity_count
    total_notifications = total_activity + moment_count
    if student_count > 0:
        notifications_per_student = total_notifications/student_count
    else:
        notifications_per_student='N/A'
    status_dict = {'students':student_count, 'linked_percentage':student_linked_percentage,
                   'active_percentage':active_guardians_percentage,
                   'moments': moment_count, 'other_activities': total_activity,
                   'notifications_per_student': notifications_per_student}
    return status_dict

def save_timeframe(start_date, end_date, type):
    daily_dict = {'start_date':start_date, 'end_date':end_date, 'type':type}
    r = mongo.timeframe.insert_one(daily_dict)
    return r.inserted_id


def save_data(doc_list):
    r = mongo.metrics.insert_many(doc_list)
    return r.inserted_ids


def get_timeframes(type, limit, page):
    timeframes = list(mongo.timeframe.find({"type":type}).sort("_id",-1).skip(limit *(page-1)).limit(limit))
    total_documents = mongo.timeframe.count_documents({"type":type})
    return (list(timeframes),total_documents)


def get_change_data(tf1, tf2,limit,page):
    tf1_metrics = list(mongo.metrics.find({"timeframe": tf1}).sort("institution",1).skip(limit *(page-1)).limit(limit))
    tf1_institutions = list(map(lambda x:x['institution'], tf1_metrics))
    tf2_metrics = list(mongo.metrics.find({'institution':{"$in": tf1_institutions},"timeframe": tf2}).sort("institution",1))
    tf1_tf2_list = [i for i in itertools.chain(tf1_metrics, tf2_metrics)]
    tf1_tf2_list.sort(key=lambda x: x['institution'])
    metrics_by_institution = itertools.groupby(tf1_tf2_list, key=lambda x: x['institution'])
    metric_list = []
    institution_list = list(mongo.institutions.find({},{'name':1,'shortid':1}))
    total_documents = mongo.metrics.count_documents({"timeframe": tf1})

    for i in institution_list:
        i['institution'] = i.pop('_id')
    for k, v in metrics_by_institution:
        m = list(v)
        for l in m:
            for i in institution_list:
                if l['institution'] == i['institution']:
                    l['institution_name'] = i['name']
                    l['institution_shortid'] = i['shortid']
        tf1_dict = next(filter(lambda x: x['timeframe'] == tf1, m), {})
        tf2_dict = next(filter(lambda x: x['timeframe'] == tf2, m), {})
        if tf2_dict:
            # replacing N/A with 0 to perform subtraction
            tf1_dict_0 = {k: 0 if v == 'N/A' else v for k, v in tf1_dict.items()}
            tf2_dict_0 = {k: 0 if v == 'N/A' else v for k, v in tf2_dict.items()}
            tf1_dict['students_delta'] = tf1_dict_0['students'] - tf2_dict_0['students']
            tf1_dict['linked_delta'] = tf1_dict_0['linked_percentage'] - tf2_dict_0['linked_percentage']
            tf1_dict['active_delta'] = tf1_dict_0['active_percentage'] - tf2_dict_0['active_percentage']
            tf1_dict['moments_delta'] = tf1_dict_0['moments'] - tf2_dict_0['moments']
            tf1_dict['other_delta'] = tf1_dict_0['other_activities'] - tf2_dict_0['other_activities']
            tf1_dict['notifications_delta'] = tf1_dict_0['notifications_per_student'] - tf2_dict_0[
                'notifications_per_student']
        else:
            tf1_dict['students_delta'] = 'N/A'
            tf1_dict['linked_delta'] = 'N/A'
            tf1_dict['active_delta'] = 'N/A'
            tf1_dict['moments_delta'] = 'N/A'
            tf1_dict['other_delta'] = 'N/A'
            tf1_dict['notifications_delta'] = 'N/A'
        metric_list.append(tf1_dict)
    return (metric_list, total_documents)


