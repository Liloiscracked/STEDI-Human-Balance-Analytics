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

# Script generated for node customer_curated
customer_curated_node1767353916594 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mo-project-customers-curated"], "recurse": True}, transformation_ctx="customer_curated_node1767353916594")

# Script generated for node step trainer landing
steptrainerlanding_node1767354516314 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mo-project-landing/step_trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1767354516314")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from step_trainer_landing join customer_curated 
on customer_curated.serialnumber = step_trainer_landing.serialNumber
'''
SQLQuery_node1767354556237 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":steptrainerlanding_node1767354516314, "customer_curated":customer_curated_node1767353916594}, transformation_ctx = "SQLQuery_node1767354556237")

# Script generated for node Select Fields
SelectFields_node1767354916942 = SelectFields.apply(frame=SQLQuery_node1767354556237, paths=["sensorReadingTime", "serialNumber", "distanceFromObject"], transformation_ctx="SelectFields_node1767354916942")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SelectFields_node1767354916942, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767352668678", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1767354961725 = glueContext.getSink(path="s3://mo-project-step-trainer-trusted", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1767354961725")
step_trainer_trusted_node1767354961725.setCatalogInfo(catalogDatabase="mo-project-database",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1767354961725.setFormat("json")
step_trainer_trusted_node1767354961725.writeFrame(SelectFields_node1767354916942)
job.commit()