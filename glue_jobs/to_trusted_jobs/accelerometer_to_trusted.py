import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_landing
accelerometer_landing_node1767297819579 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mo-project-landing/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1767297819579")

# Script generated for node customers_trusted
customers_trusted_node1767297992214 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mo-project-cutomers-trusted"], "recurse": True}, transformation_ctx="customers_trusted_node1767297992214")

# Script generated for node Join
Join_node1767297972967 = Join.apply(frame1=accelerometer_landing_node1767297819579, frame2=customers_trusted_node1767297992214, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1767297972967")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=Join_node1767297972967, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767297749683", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1767298191853 = glueContext.write_dynamic_frame.from_options(frame=Join_node1767297972967, connection_type="s3", format="json", connection_options={"path": "s3://mo-project-accelerometer-trusted", "partitionKeys": []}, transformation_ctx="accelerometer_trusted_node1767298191853")

job.commit()