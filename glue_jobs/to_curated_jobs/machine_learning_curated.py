import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1767360859981 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mo-project-step-trainer-trusted"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1767360859981")

# Script generated for node customers_curated
customers_curated_node1767360826496 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mo-project-customers-curated"], "recurse": True}, transformation_ctx="customers_curated_node1767360826496")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1767360857139 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mo-project-accelerometer-trusted"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1767360857139")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
FROM
    step_trainer_trusted
INNER JOIN
    accelerometer_trusted
    ON step_trainer_trusted.sensorReadingTime = accelerometer_trusted.timestamp
INNER JOIN
    customer_curated
    ON step_trainer_trusted.serialNumber = customer_curated.serialNumber
'''
SQLQuery_node1767361163575 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":customers_curated_node1767360826496, "accelerometer_trusted":accelerometer_trusted_node1767360857139, "step_trainer_trusted":step_trainer_trusted_node1767360859981}, transformation_ctx = "SQLQuery_node1767361163575")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767361163575, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767360769590", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1767361198469 = glueContext.getSink(path="s3://mo-project-machine-learning", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1767361198469")
machine_learning_curated_node1767361198469.setCatalogInfo(catalogDatabase="mo-project-database",catalogTableName="machine_learning_curated")
machine_learning_curated_node1767361198469.setFormat("json")
machine_learning_curated_node1767361198469.writeFrame(SQLQuery_node1767361163575)
job.commit()