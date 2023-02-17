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

# Script generated for node customer_trusted
customer_trusted_node1676649351059 = glueContext.create_dynamic_frame.from_catalog(
    database="mlp-stedi-db",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1676649351059",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="mlp-stedi-db",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1",
)

# Script generated for node accelerometer_landing_join_customer_trusted
accelerometer_landing_join_customer_trusted_node2 = Join.apply(
    frame1=accelerometer_landing_node1,
    frame2=customer_trusted_node1676649351059,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="accelerometer_landing_join_customer_trusted_node2",
)

# Script generated for node drop_customer_fields
drop_customer_fields_node1676649481889 = DropFields.apply(
    frame=accelerometer_landing_join_customer_trusted_node2,
    paths=[
        "email",
        "phone",
        "customername",
        "sharewithresearchasofdate",
        "registrationdate",
        "birthday",
        "sharewithpublicasofdate",
        "serialnumber",
        "lastupdatedate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="drop_customer_fields_node1676649481889",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node3 = glueContext.getSink(
    path="s3://mlp-stedi-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node3",
)
accelerometer_trusted_node3.setCatalogInfo(
    catalogDatabase="mlp-stedi-db", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node3.setFormat("json")
accelerometer_trusted_node3.writeFrame(drop_customer_fields_node1676649481889)
job.commit()