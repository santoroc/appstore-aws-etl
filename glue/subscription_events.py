import sys
import re

from utils.secrets import SecretsLoader
from utils.s3 import BucketPath
from utils.datetime import start_end_date, date_list_builder
from pineapple.client import AppStoreClient

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit


args = getResolvedOptions(sys.argv, ['processingBucket',
                                     'daysToFetch',
                                     'stgTable',
                                     'targetTable',
                                     'database',
                                     'landingBucket',
                                     'bucketPrefix'])

PROCESSING_BUCKET = args['processingBucket']
DAYS_TO_FETCH = args['daysToFetch']
STG_TABLE = args['stgTable']
TARGET_TABLE = args['targetTable']
DATABASE = args['database']
LANDING_BUCKET = args['landingBucket']
BUCKET_PREFIX = args['bucketPrefix']


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()


def appstore_to_s3(start_date, end_date):
    """
        Fetches sales data from the App Store API for a given
        date range and stores it in an S3 bucket.

    Args:
        start_date (str): The start date of the date range in the format 'YYYY-MM-DD'.
        end_date (str): The end date of the date range in the format 'YYYY-MM-DD'.

    Returns:
        None
    """

    secrets_loader = SecretsLoader()
    secrets = secrets_loader.get_secret('appstore')
    private_key = secrets_loader.get_secret('appstore_private_key', string_format='PLAIN TEXT')

    client = AppStoreClient(
        vendor_number=secrets['vendor_number'],
        key_id=secrets['key_id'],
        issuer_id=secrets['issuer_id'],
        private_key=private_key
    )

    bucket = BucketPath(LANDING_BUCKET, BUCKET_PREFIX)

    date_list = date_list_builder(start_date, end_date)    
    for date in date_list:
        data = client.fetch_subscription_events_report(date)
        key = f"{BUCKET_PREFIX}/{date}.csv"
        s3_uri = bucket.store(body=data, key=key)
        logger.info(f"Stored {s3_uri}")


class RedshiftLoader(SecretsLoader):
    def __init__(self, stg_table, target_table, processing_bucket,
                 start_date, end_date, database, landing_bucket, bucket_prefix):
        self.stg_table = stg_table
        self.target_table = target_table
        self.processing_bucket = processing_bucket
        self.start_date = start_date
        self.end_date = end_date
        self.database = database
        self.landing_bucket = landing_bucket
        self.bucket_prefix = bucket_prefix

        secrets = self.get_secret('redshift')
        self.host = secrets['host']
        self.port = secrets['port']
        self.username = secrets['username']
        self.password = secrets['password']


    def _cols_to_snake_case(self, df):
        for column in df.columns:
            new_col = re.sub('[^0-9a-zA-Z]+', ' ', column)
            word_list = new_col.split(' ')
            word_list = [word.lower() for word in word_list]
            new_col = '_'.join(word_list)
            df = df.withColumnRenamed(column, new_col)

        return df


    desired_schema = {
            'app_apple_id': 'string'
            ,'app_name': 'string'
            ,'cancellation_reason': 'string'
            ,'client': 'string'
            ,'consecutive_paid_periods': 'string'
            ,'country': 'string'
            ,'days_before_canceling': 'int'
            ,'days_canceled': 'int'
            ,'device': 'string'
            ,'event': 'string'
            ,'event_date': 'date'
            ,'introductory_price_duration': 'string'
            ,'introductory_price_type': 'string'
            ,'marketing_opt_in_duration': 'string'
            ,'marketing_opt_in': 'string'
            ,'original_start_date': 'date'
            ,'paid_service_days_recovered': 'string'
            ,'preserved_pricing': 'string'
            ,'previous_subscription_apple_id': 'string'
            ,'previous_subscription_name': 'string'
            ,'proceeds_reason': 'string'
            ,'promotional_offer_id': 'string'
            ,'promotional_offer_name': 'string'
            ,'quantity': 'int'
            ,'standard_subscription_duration': 'string'
            ,'state': 'string'
            ,'subscription_apple_id': 'string'
            ,'subscription_duration': 'string'
            ,'subscription_group_id': 'string'
            ,'subscription_name': 'string'
            ,'subscription_offer_duration': 'string'
            ,'subscription_offer_name': 'string'
            ,'subscription_offer_type': 'string'
        }
    

    def _conform_to_desired_schema(self, df):

        for dt in df.dtypes:
            column = dt[0]
            dtype = dt[1]
            desired_type = self.desired_schema[column]

            if column not in self.desired_schema:
                raise ValueError(f"Column {column} not in expected schema.")
            
            if dtype != desired_type:
                df = df.withColumn(column, col(column).cast(desired_type))
                logger.info(f"Converted {column} from {dtype} to {desired_type}")
        
        for c in self.desired_schema:
            if c not in df.columns:
                df = df.withColumn(c, lit(None).cast(self.desired_schema[column]))
                logger.info(F"Added {column} column.")

        return df
 

    def _read_data(self):

        df = (
            spark.read
            .options(
                delimiter='\t',
                header=True
            )
            .csv(f"s3://{self.landing_bucket}/{self.bucket_prefix}/*.csv")
        )

        df = self._cols_to_snake_case(df)
        df = self._conform_to_desired_schema(df)

        df = (
            df.withColumn('country',
                          F.when(col('country').isNull(), lit('US'))
                          .otherwise(col('country'))
            )
        )

        logger.info('Data reading done.')
        return df


    @property
    def preactions_sql(self):
        sql = """
CREATE TABLE IF NOT EXISTS %s
(
"""

        cols_list = []
        for column in self.desired_schema:
            if self.desired_schema[column] == 'string':
                cols_list.append(f"{column} varchar(255)")
            else:
                cols_list.append(f"{column} {self.desired_schema[column]}")

        sql += ',\n'.join(cols_list)

        sql += """
);

truncate %s;
"""
        return sql


    @property
    def postactions_sql(self):
        sql = f"""
delete from {self.target_table}
where event_date between '{self.start_date}' and '{self.end_date}';

insert into {self.target_table} (
"""
        
        cols_string = ',\n'.join(list(self.desired_schema.keys()))
        sql += cols_string

        sql += ",\netl_timestamp"
        sql += """
)
select
"""
        sql += cols_string
        sql += ",\n current_timestamp as etl_timestamp"
        sql += f"""
from %s
where event_date between '{self.start_date}' and '{self.end_date}';"""
        
        return sql


    def load(self):
        df = self._read_data()
        jdbc_url = f'jdbc:redshift://{self.host}:{self.port}/{self.database}'

        (
            df.write
            .format("io.github.spark_redshift_community.spark.redshift")
            .option("url", jdbc_url)
            .option("user", self.username)
            .option("password", self.password)
            .option("dbtable", self.stg_table)
            .option("tempdir", self.processing_bucket)
            .option("forward_spark_s3_credentials", "true")
            .option("preactions", self.preactions_sql)
            .option("postactions", self.postactions_sql)
            .mode('append')
            .save()
        )


def main():
    start_date, end_date = start_end_date(DAYS_TO_FETCH, days_behind=2)
    appstore_to_s3(start_date, end_date)

    redshift = RedshiftLoader(
        stg_table=STG_TABLE,
        target_table=TARGET_TABLE,
        processing_bucket=PROCESSING_BUCKET,
        start_date=app_store.start_date,
        end_date=app_store.end_date,
        database=DATABASE,
        landing_bucket=LANDING_BUCKET,
        bucket_prefix=BUCKET_PREFIX
    )

    redshift.load()
    logger.info("Job done!")


main()
job.commit()



