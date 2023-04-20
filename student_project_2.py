from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from minio import Minio
import pandas, json, smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

minio_client = Minio(
    #"127.0.0.1:9000",
    #"host.docker.internal:9000", # local host
    "localhost:9000",
    access_key="lLwtW2le7ueQu5rA",
    secret_key="B15zYgNoEbWyO7ImGXwkLKSCpSOGazCd",
    secure=False
)

pg_hook = PostgresHook(
    postgres_conn_id='local_postgres',
    schema='test'
)

pg_conn = pg_hook.get_conn()
cursor = pg_conn.cursor()

def rupiah_format(number:int):
    rupiah = str("{:,}".format(number)).replace(",", ".")
    return "Rp. " + rupiah + ",00"

def download_from_minio():
    # Download file from Minio
    minio_client.fget_object("mydataset", "product_sales.csv", "/home/airflow/product_sales.csv")

def processing_file():
    data_file = "/home/airflow/product_sales.csv"
    df = pandas.read_csv(data_file, names=("sku","name","stock"), skiprows = 1)
    result = df.to_json(orient='records')
    parsed = json.loads(result)

    for p in parsed:

        sql_stmt = "select * from product_sales where sku = '" + p['sku'] + "'"
        cursor.execute(sql_stmt)
        fetch_data = cursor.fetchone()

        if(fetch_data is None):
            sql_stmt = "INSERT INTO project_2 (sku, sold, price, baseprice) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql_stmt, p['sku'], p['sold'], p['price'], p['baseprice'])
            pg_conn.commit()
        else :
             print("SKU "+p['sku']+" already exists")   

    
    cursor.close()
    pg_conn.close()

def get_data():
    sql_stmt = "select * from product_sales"
    cursor.execute(sql_stmt)
    res = cursor.fetchall()
    return res

def generate_and_send_email():
    login = "tugasdaiva@gmail.com" # paste your login
    password = "rekexrxipdufufyz" # paste your password
    sender_email = "noreply@airflow.com"
    receiver_email = "tugasdaiva@gmail.com"
    message = MIMEMultipart("alternative")
    message["Subject"] = "Product Sales Summary"
    message["From"] = sender_email
    message["To"] = receiver_email
    # write the plain text part

    results = [
        {"sku":"A1","sold":2, "price":15000, "baseprice":10000},
        {"sku":"A2","sold":4, "price":25000, "baseprice":15000},
        {"sku":"A3","sold":7, "price":30000, "baseprice":25000},
        ]
    results = get_data()
    tableRow = ""

    for res in results:
        profit = res['sold'] * (res['price'] - res['baseprice'])
        sku = "<td>"+res['sku']+"</td>"
        sold = "<td>"+str(res['sold'])+"</td>"
        price = "<td>"+rupiah_format(res['price'])+"</td>"
        basePrice = "<td>"+rupiah_format(res['baseprice'])+"</td>"
        profitRow = "<td>"+rupiah_format(profit)+"</td>"
        tableRow += "<tr>" + sku + sold + price + basePrice + profitRow + "</tr>"

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
                    <H1>Below are the Sales Summary : </H1>
                </p>
        <table class="GeneratedTable">
        <thead>
            <tr>
                <th>SKU</th>
                <th>Sold</th>
                <th>Price</th>
                <th>Baseprice</th>
                <th>Profit</th>
            </tr>
        </thead>
        <tbody>
            """+tableRow+"""
        </tbody>
        </table>
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
    'student_project_2',
    default_args = default_args,
    schedule_interval = None
)

download_file = PythonOperator(
    task_id="download_file",
    python_callable=download_from_minio,
    dag = dag
)

process_file = PythonOperator(
    task_id="process_file",
    python_callable=processing_file,
    dag = dag
)

create_table = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'local_postgres',
    sql = '''
         create table if not exists product_sales(
        sku VARCHAR ( 50 ) NOT null PRIMARY KEY,
        sold INT NOT null default 0,
        price INT NOT null default 0,
        baseprice INT NOT null default 0
        );
    ''',
    dag = dag
)

send_email = PythonOperator(
    task_id="send_email",
    python_callable=generate_and_send_email,
    dag = dag
)

download_file >> create_table >> process_file  >> send_email
# send_email