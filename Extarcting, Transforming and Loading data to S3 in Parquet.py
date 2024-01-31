# Glue job to get two columns from two tables using join and loading the two columns df into s3 in parquet format

##----------------------------------------
## Job Name: 
## Version:
## Author:
## Date:
##----------------------------------------

##----------------------------------------
## Updates
##----------------------------------------

##----------------------------------------
## Import Packages
##----------------------------------------
import datetime

##----------------------------------------
## calling all the common functions stored in s3 as udf python library
from udf import *
import base64 as b64
import pyspark.sql.functions as F
##----------------------------------------

##----------------------------------------
## Initializations
spark.conf.set("spark.sql.legacy.timeParsePolicy", "LEGACY")

##----------------------------------------
## Job Initializations
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'ORACLE_URL',
                           'ORACLE_USER',
                           'ORACLE_PASSWORD',
                           'S3_PATH'
                           ])

##----------------------------------------
## Variable Initializations
##----------------------------------------
## Default argument of glue
##----------------------------------------
job_name = args['JOB_NAME']
run_id = args['JOB_RUN_ID']

##----------------------------------------
## S3 Connection details
##----------------------------------------
s3_path = args['S3_PATH']

##----------------------------------------
## Oracle DB Schema
##----------------------------------------
oracle_schema = ""

##----------------------------------------
## Oracle connection details
##----------------------------------------
oracle_username = args['ORACLE_USER']
oracle_password = b64.b64decode(args['ORACLE_PASSWORD']).decode()
oracle_url = args['ORACLE_URL']

##----------------------------------------
## Supporting tables from oracle
##----------------------------------------
oracle_abc = oracle_schema+".abc"
oracle_def = oracle_schema+".def"

##----------------------------------------
## Query to join table 1 and table 2 on a column
##----------------------------------------
main_query = """(SELECT * FROM
(SELECT A.COLUMN_NAME,A.COLUMN_NAME1, A.COLUMN_NAME2,
B.COLUMN_NAME, B.COLUMN_NAME1, B.COLUMN_NAME2
FROM TABLE A
LEFT JOIN (SELECT COLUNM_NAME, COLUMN_NAME1, COLUMN_NAME2 FROM TABLE B WHERE COLUMN_NAME3 = 'CONDITION') B ON A.COLUMN = B.COLUMN))"""
main_df = get_oracle_table(main_query, oracle_url, oracle_username, oracle_password, oracle_table)

##----------------------------------------
## Grouping by Column and creating a list of column1 and exploding the list with column2
##----------------------------------------
df = main_df.groupBy(['COLUMN']).agg(F.collect_set(F.col('COLUMN1')).alias('collect_list'),F.max('COLUMN2')).withColumn('exploded',F.explode('collect_list'))

##----------------------------------------
## Renaming columns as required
##----------------------------------------
df = df.withColumnRenamed("max(COLUMN2)","COLUMN2").withColumnRenamed("exploded","COLUMN1").select("COLUMN1","COLUMN2")

##----------------------------------------
## Converting DataFrame to DynamicFrame
##----------------------------------------
dyf = DynamicFrame.fromDF(df,glueContext,'dyf')

##----------------------------------------
## function to write DynamicFrame to S3 bucket in Parquet format
##----------------------------------------
def s3_write(dyf,s3_path):
  glueContext.write_dynamic_frame.from_options(
      frame = dyf,
      connectin_type = "s3",
      format = "parquet",
      connection_options = {"path":s3_path}
  )
s3_write(dyf,s3_path)