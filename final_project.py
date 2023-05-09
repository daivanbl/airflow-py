from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json, smtplib, ssl, urllib.parse, requests,time, glob

pg_hook = PostgresHook(
    postgres_conn_id='local_postgres',
    schema='test'
)

pg_conn = pg_hook.get_conn()
cursor = pg_conn.cursor()

time_end = round(time.time() * 1000)
time_start = round((time.time() - 300) * 1000)
datetimex = datetime.now().strftime("%Y-%m-%d %H:%M:00")

path = "/home/airflow/data"
directory = Path(path)
directory.mkdir(parents=True, exist_ok=True)

def encodeUriComponent(string):
    string = urllib.parse.quote(string)
    component = [
        {"utf":"%3D","char":"="},
        {"utf":"%2F","char":"/"},
        {"utf":"%2C","char":","},
        {"utf":"%21","char":"!"},
        {"utf":"%2A","char":"*"},
        {"utf":"%28","char":"("},
        {"utf":"%29","char":")"}
        ]
    for c in component:
        string = string.replace(c['utf'], c['char'])
    return string
    

def processingData():
    filecontent = ""
    grafanaQuery="SELECT COUNT(\"businessErrorInt\") AS TRX, (COUNT(\"businessErrorInt\") - SUM(\"systemErrorInt\")) AS \"SUCCESS\", SUM(\"systemErrorInt\") AS ERROR, SUM(\"businessErrorInt\") AS businessErrorInt, MEAN(\"responseTime\") AS \"AVGRT\" FROM \"homelte_log_prod\".\"default\".\"service_monitor\" WHERE  (service != 'web' AND service != 'ios' AND service != 'android') AND time >= "+str(time_start)+"ms GROUP BY \"service\",\"serviceExternal\", \"eventType\", \"node\" LIMIT 10"

    grafanaQuery = encodeUriComponent(grafanaQuery)
    url = "https://itoc.telkomsel.co.id/api/datasources/proxy/271/query?db=homelte_log_prod&q="+grafanaQuery+"&epoch=ms"
    payload={}
    headers = {
    'Authorization': 'Basic ZWxhc3RpY19nZXRfZGF0YTplbGFzdGljMjAyMQ=='
    }
    print("START CURL")
    response = requests.get(url, headers=headers, data=payload, verify=False)
    print("FINISH CURL")
    parsed = json.loads(response.content)

    if len(parsed['results'][0]) > 1:
        aggregation = parsed['results'][0]['series']
        # print(aggregation)

        for point in aggregation:
            service = point['tags']['service']
            eventType = point['tags']['eventType']
            eventType = str(eventType).replace("|",":")
            serviceExternal = point['tags']['serviceExternal']
            trx = point['values'][0][1]
            success = point['values'][0][2]
            error = point['values'][0][3]
            filecontent += "|"+str(datetimex)+"|"+service+"|"+eventType+"|"+serviceExternal+"|"+str(trx)+"|"+str(success)+"|"+str(error)+"\n"
    else:
        print("KOSONG")

    with open(path+"/loadtodb.txt", 'w') as f:
        f.write(filecontent)
        f.close()

def insertData():
    with open(path+"/loadtodb.txt", 'r') as f:
     global datetimex
    # print(f.read())
     for line in f.readlines():
         value = line.split('|')
         params = (value[1], value[2], value[3], value[4], value[5], value[6], value[7].replace('\n', ''))
         datetimex = value[1]
         sql_stmt = "INSERT INTO service_monitoring (datetimex, service, eventType, serviceExternal, trx, success, error) VALUES (%s, %s, %s, %s, %s, %s, %s)"
         cursor.execute(sql_stmt, params)
         pg_conn.commit()

    cursor.close()
    pg_conn.close()

# def insertData():
#     f = open(path+"/loadtodb.txt", "r")
#     print(f.read())
#     filename = str(path+"/loadtodb.txt")
#     # sql_stmt = "load data local infile '%s' into table service_monitoring FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';"
#     sql_stmt = "COPY service_monitoring FROM '"+filename+"' (DELIMITER('|'));"
#     cursor.execute(sql_stmt)
#     pg_conn.commit()

#     cursor.close()
#     pg_conn.close()

def get_data(date):
    sql_stmt = "select service, sum(trx), sum(error) from service_monitoring where datetimex = '"+date+"' and error > 0 group by service"
    # sql_stmt = "select service, sum(trx), sum(error) from service_monitoring where datetimex = '2023-05-09 04:12:00' group by service"
    print(sql_stmt)
    cursor.execute(sql_stmt)
    res = cursor.fetchall()
    return res

def generate_and_send_email():
    login = "tugasdaiva@gmail.com" # paste your login
    password = "rekexrxipdufufyz" # paste your password
    sender_email = "noreply@airflow.com"
    receiver_email = "tugasdaiva@gmail.com"
    message = MIMEMultipart("alternative")
    message["Subject"] = "Alert Error Transaction"
    message["From"] = sender_email
    message["To"] = receiver_email
    # write the plain text part

    results = get_data(datetimex)

    if(len(results) == 0):
        print('NO ERRORS')
    else :
        content = ""

        for res in results:
            content += "<div> Service " + res[0] +" : "+ str(res[2]) + " Errors from "+ str(res[1]) +" TRX</div><br>"
        print(content)
        html = """\
        <html>
        <head>
            <!-- CSS Code: Place this code in the document's head (between the 'head' tags) -->
            <style>
            table.GeneratedTable {
            width: 100%;
            background-color: #ffffff;
            border-collapse: collapse;
            border-width: 2px;
            border-color: #000000;
            border-style: solid;
            color: #000000;
            }

            table.GeneratedTable td, table.GeneratedTable th {
            border-width: 2px;
            border-color: #000000;
            border-style: solid;
            padding: 3px;
            }

            table.GeneratedTable thead {
            background-color: #ababab;
            }
            </style>
        </head>
        <body>
            <!-- HTML Code: Place this code in the document's body (between the 'body' tags) where the table should appear -->
                    <p>
                        <H1>Alert Error Transaction """+str(datetimex)+""" : </H1>
                    </p>
                """+content+"""
            
        </body>
        </html>
        """
        # convert both parts to MIMEText objects and add them to the MIMEMultipart message
        part2 = MIMEText(html, "html")
        message.attach(part2)
        context=ssl.create_default_context()
        # send your email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(login, password)
            server.sendmail(
                sender_email, receiver_email, message.as_string()
            )
        print('Sent')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,4,20),
    'depends_on_past': False,
    'retries': 0
}

dag = DAG(
    'final_project',
    default_args = default_args,
    schedule_interval = None
)

processing_data = PythonOperator(
    task_id="processing_data",
    python_callable=processingData,
    dag = dag
)

create_table = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'local_postgres',
    sql = '''
    create table if not exists service_monitoring(
         id SERIAL PRIMARY KEY,
        datetimex TIMESTAMP NULL,
        service varchar(255) NULL,
        eventType varchar(255)  NULL,
        serviceExternal varchar(255) NULL,
        trx INT NULL,
        success INT NULL,
        error INT NULL
        );
    ''',
    dag = dag
)

insert_data = PythonOperator(
    task_id="insert_data",
    python_callable=insertData,
    dag = dag
)

send_email = PythonOperator(
    task_id="send_email",
    python_callable=generate_and_send_email,
    dag = dag
)
#         UNIQUE (datetime,service,eventType,serviceExternal)
processing_data  >> create_table >> insert_data >> send_email
# send_email