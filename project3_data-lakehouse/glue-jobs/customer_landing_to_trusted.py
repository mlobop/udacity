import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mlp-stedi-lakehouse/customer/landing"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1",
)

# Script generated for node filter_null_researchdates
filter_null_researchdates_node2 = Filter.apply(
    frame=accelerometer_landing_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="filter_null_researchdates_node2",
)

# Script generated for node customer_trusted
customer_trusted_node3 = glueContext.getSink(
    path="s3://mlp-stedi-lakehouse/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node3",
)
customer_trusted_node3.setCatalogInfo(
    catalogDatabase="mlp-stedi-db", catalogTableName="customer_trusted"
)
customer_trusted_node3.setFormat("json")
customer_trusted_node3.writeFrame(filter_null_researchdates_node2)
job.commit()