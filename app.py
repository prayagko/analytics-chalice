from chalice import Chalice, Response, Cron
from chalicelib import report, linking, common, activity,status
from datetime import datetime, timedelta
from bson.objectid import ObjectId

app = Chalice(app_name='analytics')
app.debug = True


@app.route('/')
def index():
    return {'Hello': 'World'}


@app.route('/institutions', cors=True)
def get_institutions():
    institutions = common.get_institution_dict()
    return {'institutions', institutions}


@app.route('/report', cors=True)
def get_report():
    request = app.current_request
    args = request.query_params
    if args and 'startdate' in args and 'enddate' in args and 'institution' in args and 'activities' in args:
        start_date = args.get('startdate')
        end_date = args.get('enddate')
        activity_list = args.get('activities').split(",")
        shortid = args.get('institution')
        oid= common.get_oid_from_shortid(shortid)
        interval = common.get_date_interval(start_date, end_date)
        report_list = report.get_activity_report(activity_list, interval, oid)
        print(report_list)
        return {'data': report_list}
    else:
        return Response(body='Invalid params!', status_code=400)


@app.route('/linking', cors=True)
def get_linked_unlinked():
    request = app.current_request
    args = request.query_params
    if args and 'institution' in args:
        institution = args['institution']
        oid = common.get_oid_from_shortid(institution)
        info = linking.get_linked_unlinked_info(oid)
        return {'data': info}
    else:
        return Response(body='Invalid params!', status_code=400)


@app.route('/activity', cors=True)
def get_active_inactive():
    request = app.current_request
    args = request.query_params
    if args and 'startdate' in args and 'enddate' in args and 'institution' in args and 'type' in args and 'number' in args:
        institution = args['institution']
        start_date = args.get('startdate')
        end_date = args.get('enddate')
        interval = common.get_date_interval(start_date, end_date)
        type = args.get('type')
        number = args.get('number')
        oid = common.get_oid_from_shortid(institution)
        active_users = activity.get_active_users(oid, interval)
        user_list = list(map(lambda x: x['_id'], active_users))
        guardians = activity.guardians_from_users(user_list)
        guardian_list = list(map(lambda x: x['_id'], guardians))
        students = activity.get_students_from_guardians(guardian_list)
        if type == 'active':
            if number == 'true':
                students = activity.get_guardian_numbers(students)
            # the below line turns the objectid type values to string so we can send it in the response
            response = [{k: str(v) for k, v in i.items()} for i in students]
        else:

            student_list = list(map(lambda x: x['_id'], students))
            inactive_students = activity.get_inactive_students(student_list,oid)
            if number == 'true':
                inactive_students = activity.get_inactive_with_numbers(inactive_students)
            # the below line turns the objectid type values to string so we can send it in the response
            response = [{k: str(v) for k, v in i.items()} for i in inactive_students]
        return {"data": response}
    else:
        return Response(body='Invalid params!', status_code=400)


@app.route('/status', cors=True)
def get_institution_status():
    request = app.current_request
    args = request.query_params
    if args and 'institution' in args and 'startdate' in args and 'enddate' in args and 'activities' in args:
        institution = args.get('institution')
        start_date = args.get('startdate')
        end_date = args.get('enddate')
        interval = common.get_date_interval(start_date, end_date)
        activities = args.get('activities').split(",")
        oid = common.get_oid_from_shortid(institution)
        status_dict = status.get_status_metrics(oid, interval, activities)
        return status_dict
    else:
        return Response(body='Invalid params!', status_code=400)


# @app.route('/daily', cors=True)
@app.schedule(Cron(00, 17, '?', '*', 'SUN-FRI', '*'))
def save_daily_metrics(event):
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=1)
    interval = common.get_date_interval(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    inserted_id = status.save_timeframe(start_date, end_date,'daily')
    institutions = common.get_institution_dict()
    activities = ['food', 'homework', 'remarks']
    doc_list = []
    for key, value in institutions.items():
        oid = common.get_oid_from_shortid(value)
        status_dict = status.get_status_metrics(oid, interval, activities)
        status_dict['timeframe'] = inserted_id
        status_dict['institution'] = oid
        doc_list.append(status_dict)
    r = status.save_data(doc_list)
    return {'documents': str(r)}


# @app.route('/weekly', cors=True)
@app.schedule(Cron(15, 17, '?', '*', 'SAT', '*'))
def save_weekly_metrics(event):
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=7)
    interval = common.get_date_interval(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    inserted_id = status.save_timeframe(start_date, end_date, 'weekly')
    institutions = common.get_institution_dict()
    activities = ['food', 'homework', 'remarks']
    doc_list = []
    for key, value in institutions.items():
        oid = common.get_oid_from_shortid(value)
        status_dict = status.get_status_metrics(oid, interval, activities)
        status_dict['timeframe'] = inserted_id
        status_dict['institution'] = oid
        doc_list.append(status_dict)
    r = status.save_data(doc_list)
    return {'documents': str(r)}


@app.route('/last-week-change', cors=True)
def get_last_week_change():
    request = app.current_request
    args = request.query_params
    if args and 'limit' in args and 'page' in args:
        limit = int(args.get('limit'))
        page = int(args.get('page'))
        tf1_tf2 = status.get_timeframes('weekly', 2, 1)[0]
        tf1_tf2 = list(map(lambda x: x['_id'], tf1_tf2))
        tf1 = tf1_tf2[0]
        if len(tf1_tf2) == 2:
            tf2 = tf1_tf2[1]
        else:
            return {'Error': 'Only one timeframe available'}
        change_data, total_documents = status.get_change_data(tf1, tf2, limit, page)
        response = [{k: str(v) for k, v in i.items()} for i in change_data]
        return {'data': response,'page-info':{'limit':limit,'page':page,'total':total_documents}}
    else:
        return Response(body="Invalid params! No limit and page params provided", status_code=400)


@app.route('/timeframes', cors=True)
def get_timeframes_list():
    request = app.current_request
    args = request.query_params
    if args and 'limit' in args and 'type' in args and 'page' in args:
        type = args.get('type')
        limit = int(args.get('limit'))
        page = int(args.get('page'))
        timeframes, total_documents = status.get_timeframes(type, limit, page)
        print('timeframes: ', timeframes)
        print('total: ',total_documents)
        for t in timeframes:
            d = {k:str(v)for (k,v) in t.items()}
            t.update(d)
        return {'data': timeframes,'page-info':{'limit':limit,'page':page,'total':total_documents}}
    else:
        return Response(body="Invalid params! limit, page and type params provided", status_code=400)


@app.route('/any-timeframe-change', cors=True)
def get_any_timeframe_change():
    request = app.current_request
    args = request.query_params
    if args and 'new-tf' in args and 'old-tf' in args and 'limit' in args and 'page' in args:
        limit = int(args.get('limit'))
        page = int(args.get('page'))
        tf1 = ObjectId(args.get('new-tf'))
        tf2 = ObjectId(args.get('old-tf'))
        change_data, total_documents = status.get_change_data(tf1, tf2, limit, page)
        response = [{k: str(v) for k, v in i.items()} for i in change_data]
        return {'data': response, 'page-info':{'limit':limit,'page':page,'total':total_documents}}
    else:
        return Response(body='Invalid params! New-tf, old-tf, limit and page params should be given', status_code=400)
