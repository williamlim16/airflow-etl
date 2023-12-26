from airflow.decorators import dag, task
from datetime import timedelta,datetime
from airflow.models import Variable
import psycopg2
import pandas as pd

sales_source = Variable.get("csv_path")
sales_target = Variable.get("target_path")

default_args = {
  'owner': 'william',
  'retries': 5,
  'retry_delay': timedelta(minutes=5),
}

@dag(
  dag_id='extract-transform-load',
  default_args=default_args,
  start_date=datetime(2023,12,10),
  schedule_interval='@daily',
)
def extranct_transform_load_demo():

  @task()
  def transform():
    amz = pd.read_csv(sales_source)
    sales = amz.loc[:,["product_id", "product_name", "category", "actual_price","discounted_price"]]
    sales['actual_price'] = pd.to_numeric(sales['actual_price'].str[1:].replace(',','', regex=True),errors='coerce')
    sales['discounted_price'] = pd.to_numeric(sales['discounted_price'].str[1:].replace(',','', regex=True),errors='coerce')
    sales['product_name'] = sales['product_name'].str.replace(';','')
    sales.drop_duplicates(inplace=True)
    sales.to_csv(sales_target, sep=";", header=False, index=False)

  @task()
  def load():
    connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="db",
                                      port="5432",
                                      database="products")
    cursor = connection.cursor()
    cursor.execute(
        """
        DROP TABLE IF EXISTS sales CASCADE;
        """)
    connection.commit()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
          product_id  varchar(255) NOT NULL,
          product_name text NOT NULL,
          category varchar(255) NOT NULL,
          actual_price numeric NOT NULL,
          discounted_price numeric NOT NULL
        )
        """)
    connection.commit()
    f = open(sales_target, 'r')
    cursor.copy_from(f, 'sales', sep=';' )
    connection.commit()

  @task()
  def analyze():
    connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="db",
                                      port="5432",
                                      database="products")
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE MATERIALIZED VIEW total_sales
        AS
        SELECT sum(actual_price) from sales;
        """)
    connection.commit()

    cursor.execute(
        """
        CREATE MATERIALIZED VIEW top5_expensive
        AS
        SELECT product_id, product_name, actual_price FROM sales
        ORDER BY actual_price DESC
        LIMIT 5;
        """)
    connection.commit()

    cursor.execute(
        """
        CREATE MATERIALIZED VIEW average_discount
        AS
        SELECT AVG(discounted_price) FROM sales;
        """)
    connection.commit()

  transform() >> load() >> analyze()

final_dag = extranct_transform_load_demo()