import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


###READ LAST DAY'S DELTA FROM S3 AND GET MAX DATE
# df_yester_delta = spark.read.format('parquet').load("s3://ccsm-prod-rawdata/msil_ccsm_table_Finance/msilmdm_policy_df_today_incr/")
# print (f"df_yester_delta_count : {df_yester_delta.count()}")
# df_yester_delta.createOrReplaceTempView("df_yester_delta")
# max_date = spark.sql('''
# Select max(updated_date) from df_yester_delta ''')
# print (f"df_yester_delta_count : {df_yester_delta.count()}")


###READ RECORDS FROM CATALOGUE WHICH IS GREATER THAN MAX_DATE OF LAST DAY'S DELTA
datasource = glueContext.create_dynamic_frame.from_catalog(database = "msil_datalake_curated_mdm_dblink", table_name = "msilmdm_policy")
df = datasource.toDF()
df = df.drop("tp_converted_by")
df.createOrReplaceTempView("df")
# df_today = spark.sql('''select * from df 
# where updated_date > (Select max(updated_date) from df_yester_delta)''')
df_today = spark.sql('''select * from df 
where updated_date > '2024-04-19 23:21:37' ''')
#df_today.printSchema()
print (f"df_today_count : {df_today.count()}")


###OVERWRITE DELTA RECORDS TO S3
df_today.write\
        .option("header" , True)\
        .mode("overwrite")\
        .parquet("s3://ccsm-prod-rawdata/msil_ccsm_table_Finance/msilmdm_policy_df_today_incr/")
