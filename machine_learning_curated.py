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
AmazonS3_node1755557851076 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://elivia-trusted-zone/accelerometer_trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1755557851076")

# Script generated for node Amazon S3
AmazonS3_node1755557870622 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://elivia-trusted-zone/step_trainer_trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1755557870622")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
  st.sensorReadingTime AS timestamp,   -- output key
  st.sensorReadingTime,
  st.distanceFromObject,
  acc.x,
  acc.y,
  acc.z,
  acc.`user`
FROM step_trainer_trusted st
INNER JOIN accelerometer_trusted acc
  ON st.sensorReadingTime = acc.timeStamp;
'''
SQLQuery_node1755557897448 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":AmazonS3_node1755557851076, "step_trainer_trusted":AmazonS3_node1755557870622}, transformation_ctx = "SQLQuery_node1755557897448")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755557994566 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1755557897448, database="curated_zone_database", table_name="machine_learning_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1755557994566")

job.commit()