import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1755547574988 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://elivia-trusted-zone/customer_trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1755547574988")

# Script generated for node Amazon S3
AmazonS3_node1755547594964 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://elivia-trusted-zone/accelerometer_trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1755547594964")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT c.customerName,
       c.email,
       c.phone,
       c.birthDay,
       c.serialNumber,
       c.registrationDate,
       c.lastUpdateDate,
       c.shareWithResearchAsOfDate,
       c.shareWithPublicAsOfDate,
       c.shareWithFriendsAsOfDate
FROM customer_trusted c
LEFT SEMI JOIN accelerometer_trusted a
  ON lower(trim(c.email)) = lower(trim(a.`user`));
'''
SQLQuery_node1755543691332 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":AmazonS3_node1755547574988, "accelerometer_trusted":AmazonS3_node1755547594964}, transformation_ctx = "SQLQuery_node1755543691332")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755543746222 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1755543691332, database="curated_zone_database", table_name="customer_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1755543746222")

job.commit()