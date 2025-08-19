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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755628930677 = glueContext.create_dynamic_frame.from_catalog(database="curated_zone_database", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1755628930677")

# Script generated for node Amazon S3
AmazonS3_node1755629059770 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://elivia-landing-zone/step_trainer_landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1755629059770")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
FROM step_trainer_landing
WHERE serialNumber IN (
    SELECT serialNumber
    FROM customer_curated
);
'''
SQLQuery_node1755540923161 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":AWSGlueDataCatalog_node1755628930677, "step_trainer_landing":AmazonS3_node1755629059770}, transformation_ctx = "SQLQuery_node1755540923161")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755541352417 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1755540923161, database="trusted_zone_database", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1755541352417")

job.commit()