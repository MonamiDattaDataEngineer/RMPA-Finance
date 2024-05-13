import sys
import datetime, pytz
from datetime import timedelta
import pandas as pd
import boto3
import traceback
import json
import psycopg2


    
try:
	con = psycopg2.connect(dbname='ccsm_db', port='5439' ,user='ccsm_user', password='ccSm@1234', host='ace-common-cluster-1.csuh9tvc69yu.ap-south-1.redshift.amazonaws.com')
	cur = con.cursor()
	print("Redshift connnection created")
except Exception as e:
	print("Exception while creating connection to Redshift Cluster")
	print(e)
    




table_info = [
# {"s3_path": "s3://ccsm-prod-rawdata/msil_ccsm_table_2/msilmdm_policy_vehicle/", "table": "msilmdm_policy_vehicle"}]
# {"s3_path": "s3://ccsm-prod-rawdata/msil_ccsm_table_2/msilmdm_policy_extended/", "table": "msilmdm_policy_extended"}]
{"s3_path": "s3://ccsm-prod-rawdata/msil_ccsm_table_Finance/msilmsdm_policy_df_today_incr/", "table": "msilmdm_policy_new_delta"}]


for info in table_info: 
    table = info["table"]
    project_path = info["s3_path"]
    transform_schema = 'public'  
    iam_role = 'arn:aws:iam::825589354750:role/redshift_role1,arn:aws:iam::903493690682:role/AWS-Glue-Role-CCSM-Prod'
    try:
        trunc_staging_query = f'TRUNCATE TABLE {transform_schema}.{table};END;'
        copy_command = f''' COPY {transform_schema}.{table} 
                            FROM '{project_path}' 
                            IAM_ROLE '{iam_role}'
                            FORMAT AS PARQUET;END;'''
                            
        cur.execute(trunc_staging_query)
        cur.execute(copy_command)
        con.commit()
        print(f'Data inserted into PROD for table {table}')
        
    except Exception as e:
        print(f'Exception while Ingesting data to PROD for table {table}')
        print(e)
        con.commit()




print("job completed")




    