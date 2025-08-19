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
AmazonS3_node1755629854524 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://elivia-landing-zone/customer_landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1755629854524")

# Script generated for node SQL Query
SqlQuery0 = '''
Select *
From customer_landing
where sharewithresearchasofdate IS NOT NULL;
'''
SQLQuery_node1755479186147 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":AmazonS3_node1755629854524}, transformation_ctx = "SQLQuery_node1755479186147")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755532640480 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1755479186147, database="trusted_zone_database", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1755532640480")

job.commit()