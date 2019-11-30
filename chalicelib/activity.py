from chalicelib import db

mongo = db.mongo


def get_active_users(oid, interval):
    active_users = mongo.users.find({'lastActive': {'$gte': interval[0], '$lt': interval[1]}, 'institution':oid},{'_id':1})
    return list(active_users)


def guardians_from_users(user_list):
    guardians = mongo.guardians.find({'linking.token.user':{"$in": user_list}},{'_id':1, 'mobileNumber':1})
    return list(guardians)


def get_students_from_guardians(guardian_list):
    students = mongo.students.find({'guardians.guardian':{"$in": guardian_list}},{'guardians':1,'name':1})
    return list(students)


def get_guardian_numbers(student_list):
    for student in student_list:
        for g in student.get('guardians'):
            guard = mongo.guardians.find_one({'_id': g.get('guardian')}, {'mobileNumber': 1})
            if guard:
                g['mobileNumber'] = guard.get('mobileNumber')
    return student_list

def get_inactive_students(student_list,oid):
    inactive_students = list(mongo.students.find({'_id':{"$nin":student_list}, "institution":oid},{'guardians':1,'name':1}))
    return inactive_students

def get_inactive_with_numbers(student_list):
    for student in student_list:
        for g in student.get('guardians'):
            guard = mongo.guardians.find_one({'_id': g.get('guardian')}, {'mobileNumber': 1})
            if guard:
                g['mobileNumber'] = guard.get('mobileNumber')
    return student_list


