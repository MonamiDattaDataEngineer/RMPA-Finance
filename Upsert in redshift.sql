import sys
import datetime, pytz
from datetime import timedelta
import pandas as pd
import boto3
import traceback
import json
import psycopg2


    
import psycopg2
import boto3
import traceback
import json
import sys
from awsglue.utils import getResolvedOptions

try:
	con = psycopg2.connect(dbname='ccsm_db', port='5439' ,user='ccsm_user', password='ccSm@1234', host='ace-common-cluster-1.csuh9tvc69yu.ap-south-1.redshift.amazonaws.com')
	cur = con.cursor()
	print("Redshift connnection created")
except Exception as e:
	print("Exception while creating connection to Redshift Cluster")
	print(e)




table_info = [
# { "table": "msilmdm_policy_vehicle"}]
# { "table": "msilmdm_policy_extended"}]
{ "table": "msilmdm_policy_new"}]



 
iam_role = 'arn:aws:iam::825589354750:role/redshift_role1,arn:aws:iam::903493690682:role/AWS-Glue-Role-CCSM-Prod'

query = """
    begin;
    delete from public.msilmdm_policy_new using public.msilmdm_policy_new_delta where public.msilmdm_policy_new_delta.current_policy_no = public.msilmdm_policy_new.current_policy_no; 
    insert into public.msilmdm_policy_new select * from public.msilmdm_policy_new_delta;END; """

           
cur.execute(query)
print("Deleted from Inserted records in msilmdm_policy_new")
con.commit()
