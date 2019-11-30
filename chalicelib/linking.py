from chalicelib import db

mongo = db.mongo

def guardian_link_check(oid):
    db_guardian = mongo.guardians.find_one({'_id': oid})
    if 'linking' in db_guardian:
        issue_status = db_guardian['linking']['token']['status']
        if issue_status == 'linked':
            return 'linked'
    return 'issued'

def get_guardian_mobile_token(oid):
    guardian_ref = mongo.guardians.find_one({'_id': oid})
    if 'mobileNumber' in guardian_ref:
        number = guardian_ref['mobileNumber']
    else:
        number = ''
    if 'linking' in guardian_ref:
        token_code = guardian_ref['linking']['token']['code']
    else:
        token_code = ''
    return number, token_code

def get_linked_unlinked_info(oid):
    info_list = []
    for student in mongo.students.find({'institution': oid}):
        name = student['name']
        info_dict = {}
        info_dict['name'] = name
        if student['guardians']:
            for guardian in student['guardians']:
                if guardian['guardian']:
                    guardian_oid = guardian['guardian']
                    if guardian['relation']:
                        relation = guardian['relation'].lower()
                    else:
                        guardian['relation'] = 'N/A'
                    number_token = get_guardian_mobile_token(guardian_oid)
                    info_dict['shortid'] = student.get('shortid')
                    info_dict[relation + "s status"] = guardian_link_check(guardian_oid)
                    info_dict[relation + "s number"] = number_token[0]
                    info_dict[relation + "s token"] = number_token[1]
        info_list.append(info_dict)
    return info_list