####1st job_Incre_lf_tos3###########

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.functions import date_sub

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


##Reading from catalogue
datasource = glueContext.create_dynamic_frame.from_catalog(database = "msil_datalake_curated_mdm_dblink", table_name = "msilmdm_policy")
df = datasource.toDF()

##Dropping this column from catalogue as Redshift table and also fulload table doesnt have this column
df = df.drop("tp_converted_by")
df.createOrReplaceTempView("df")
print (f"df_count : {df.count()}")

print('############catalogue table read done##########')


#Reading from where HistoryLoad of yesterday(18/04/24) is done
######History Record source path is where today's final record is getting updated(i.e,Today's write path)
# df_history = spark.read.format('parquet').load("s3://ccsm-prod-rawdata/msil_ccsm_table_2/msilmdm_policy/") 
# df_history.createOrReplaceTempView("df_history")
# #max_time =df_history.agg(max(col("created_date").collect()[][]
# print (f"df_history_count : {df_history.count()}")

print('################history table read##########################")')


##From Catalogue table Reading only Incremental(i.e,Today's data which came after fulload)
#df_inc = df.filter(col("created_date") = n-1 )|(col("updated_date") = n-1)
df_inc = spark.sql('''
select * from df 
where updated_date > '2024-04-17 22:24:20' ''')

# elect max(created_date) from df_history) or updated_date > (Select max(created_date) from df_history)''')
# df_inc.createOrReplaceTempView("df_inc")
# df_inc.printSchema()(S
print (f"df_inc_count : {df_inc.count()}")


print('###########inc data is prepared################')


# df_inc_2 = spark.sql('''
# select * from df 
# where created_date > updated_date > (Select max(created_date) from df_history)''')
# df_inc_2.createOrReplaceTempView("df_inc_2")

# print (f"df_inc_2_count : {df_inc_2.count()}")


# ##Deleting those records from History which are present in Delta record in incremental
# hist_del = df_history.join(df_inc,(df_history.current_policy_no==df_inc.current_policy_no),"left_anti")
# hist_del.printSchema()

print('###################delete from history table#####################')

# hist_del=spark.sql('''
# DELETE from df_history
# where current_policy_no in (SELECT current_policy_no from df_inc) ''')
# hist_del.createOrReplaceTempView("hist_del")


##Inserting all Modified and New records present in Delta into History(yesterday's fulload) Records
# df_final = hist_del.unionAll(df_inc)
# print (f"df_final count : {df_final.count()}")

print('########################union done############################')



df_inc.write\
        .option("header" , True)\
        .mode("overwrite")\
        .parquet("s3://ccsm-prod-rawdata/msil_ccsm_table_Finance/msilmdm_policy_df_inc/")
            # print(' Data inserted into s3 for', info["table"])4

print("successful")


job.commit()









