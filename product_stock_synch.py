from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pathlib import Path
import pandas, json, glob

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,4,2),
    'depends_on_past': False,
    'retries': 0
}

dag = DAG(
    'product_stock_synch',
    default_args = default_args,
    schedule_interval = None
)
data_path = "/home/airflow/data"
data_file = glob.glob(str(data_path+"/data.csv"), recursive=True)
df = pandas.read_csv(data_file, names=("sku","name","stock"), skiprows = 1)
result = df.to_json(orient='records')
parsed = json.loads(result)

# parsed = [
#     {"sku":"A1","name":"pants","stock":-3},
#     {"sku":"A5","name":"kaos","stock":5},
#     {"sku":"A4","name":"topi","stock":3},
#     ]

def calculateStock(oldStock, newStock):
    return 0 if oldStock + newStock < 0 else oldStock + newStock

def updateStock(data):

    pg_hook = PostgresHook(
        postgres_conn_id='local_postgres',
        schema='test'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    for p in data:
        sku = p['sku']
        name = p['name']

        sql_stmt = "select * from products where sku = '" + sku + "'"
        cursor.execute(sql_stmt)
        fetch_data = cursor.fetchone()

        if(fetch_data is None):
            stock = 0 if p['stock'] < 0 else p['stock']
            sql_stmt = "INSERT INTO products (sku,name,stock) VALUES  ('"+ p['sku'] + "','" + p['name'] + "',"+ str(stock) +")"
        else :
            stock = calculateStock(fetch_data[2], p['stock'])
            sql_stmt = "UPDATE products SET name = '"+ name +"', stock="+ str(stock) + "where sku = '"+ sku +"'"

        cursor.execute(sql_stmt)
        pg_conn.commit()
    
    cursor.close()
    pg_conn.close()

update_stock = PythonOperator(
    task_id="update_stock",
    python_callable=updateStock,
    op_kwargs={'data': parsed},
    dag = dag
)

delete_file = BashOperator(
    task_id = 'delete_file',
    bash_command = 'rm $(ls -t /home/airflow/data/*.csv | head -n1)',
    dag = dag
)

done = BashOperator(
    task_id = 'done',
    bash_command = 'echo "Done"',
    dag = dag
)

# t1 = BashOperator(
#     task_id = 'test',
#     bash_command = 'ls /home/airflow/data/',
#     dag = dag
# )

update_stock >> delete_file >> done