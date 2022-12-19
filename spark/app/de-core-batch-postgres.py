from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, unix_timestamp

from dotenv import load_dotenv
import os
import sys
from pathlib import Path
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import logging

from config import tables_metadata

# TODO: Log messages to file
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)s :: %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('py4j').setLevel(logging.INFO)
logging.getLogger('pyspark')

load_dotenv()

BASE_DIR = os.path.dirname(__file__)
BASE_DATA_DIR = os.path.join(BASE_DIR,'../../data/input/tpcds_data_5g_batch')

DEFAULT_BATCH_DATE = '1998-04-01'
TABLES_LIST = ['customer', 'customer_address', 'date_dim', 'household_demographics', 'item', 'promotion',
                'store', 'store_sales', 'time_dim']

SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
PG_JAR_PATH = '/opt/spark/jars/postgresql-42.5.1.jar'

PG_HOST=os.getenv('PG_HOST')
PG_PORT=os.getenv('PG_PORT')
PG_DB=os.getenv('PG_DB')
PG_SCHEMA=os.getenv('PG_SCHEMA')
PG_USERNAME=os.getenv('PG_USERNAME', 'no_username')
PG_PASSWORD=os.getenv('PG_PASSWORD', 'no_password')

def get_batch_date():
    with open(os.path.join(BASE_DIR, 'batch_date_var.txt'), 'r') as file:
        batch_date = file.readline()

        logging.info(f'Current batch_date: {batch_date}')

    return batch_date

def set_next_batch_date(batch_date):
    batch_date_dt = dt.strptime(batch_date, '%Y-%m-%d')

    next_batch_date = dt.strftime(batch_date_dt + relativedelta(months=+3), '%Y-%m-%d')

    with open(os.path.join(BASE_DIR, 'batch_date_var.txt'), 'w') as file:
        file.write(next_batch_date)

        logging.info(f'Next batch_date: {next_batch_date}')

def get_file_dir_path(table, batch_date):
    return BASE_DATA_DIR + '/batch_' + batch_date + '/' + table

def save_data(df, table, mode='overwrite'):
    df.write.format("jdbc")\
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", f"{PG_SCHEMA}.{table}") \
        .option("user", PG_USERNAME).option("password", PG_PASSWORD) \
        .save(mode=mode)

    logging.info(f'Table `{PG_SCHEMA}.{table}` saved to DB successfully.')

def load_file(spark, file_dir_path, schema):
    if os.path.isdir(file_dir_path):
        df = spark.read.option('delimiter', '|') \
                                    .schema(schema) \
                                    .csv(file_dir_path)

        logging.info(f'File retrieved from {file_dir_path}')

        # df.printSchema() # TODO: Comment out after testing

        return df
    else:
        logging.error(f'Directory `{file_dir_path}` does not exists. Ending process.')
        sys.exit()

def load_table(spark, table):
    df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
            .option("dbtable", f"{PG_SCHEMA}.{table}") \
            .option("user", PG_USERNAME) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()

    df.logging.infoSchema()

    return df

def table_exists(spark, table):
    df = spark.read.jdbc(url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}",
                    table = f"(select table_catalog, table_schema, table_name from information_schema.tables " +
                    f"where table_catalog = '{PG_DB}' and table_schema = '{PG_SCHEMA}' and table_name = '{table}') as my_table",
                    properties={"user": PG_USERNAME, "password": PG_PASSWORD, "driver": 'org.postgresql.Driver'})

    return df.count() == 1

if __name__ == '__main__':
    batch_date = get_batch_date()

    spark = SparkSession \
        .builder \
        .master(SPARK_MASTER) \
        .appName("Spark Big Data DE Core Batch") \
        .config("spark.jars", PG_JAR_PATH) \
        .getOrCreate()

    for table_name, table_metadata in tables_metadata.items():
        schema, _ , mode = table_metadata.values()

        file_dir_path = get_file_dir_path(table_name, batch_date)
        file_data = load_file(spark, file_dir_path, schema)

        processed_data = file_data.withColumn('batch_date', unix_timestamp(lit(batch_date), 'yyyy-MM-dd').cast("timestamp"))

        save_data(processed_data, table_name, mode)

    set_next_batch_date(batch_date)