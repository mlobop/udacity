import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="mlp-stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1676654491317 = glueContext.create_dynamic_frame.from_catalog(
    database="mlp-stedi-db",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1676654491317",
)

# Script generated for node accelerometer_trusted_join_step_trainer_trusted
accelerometer_trusted_join_step_trainer_trusted_node1676654655686 = Join.apply(
    frame1=accelerometer_trusted_node1,
    frame2=step_trainer_trusted_node1676654491317,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="accelerometer_trusted_join_step_trainer_trusted_node1676654655686",
)

# Script generated for node drop_sensorreadingtime
drop_sensorreadingtime_node1676655028568 = DropFields.apply(
    frame=accelerometer_trusted_join_step_trainer_trusted_node1676654655686,
    paths=["sensorreadingtime"],
    transformation_ctx="drop_sensorreadingtime_node1676655028568",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.getSink(
    path="s3://mlp-stedi-lakehouse/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node3",
)
machine_learning_curated_node3.setCatalogInfo(
    catalogDatabase="mlp-stedi-db", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node3.setFormat("json")
machine_learning_curated_node3.writeFrame(drop_sensorreadingtime_node1676655028568)
job.commit()