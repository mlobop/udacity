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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="mlp-stedi-db",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1676651697036 = glueContext.create_dynamic_frame.from_catalog(
    database="mlp-stedi-db",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1676651697036",
)

# Script generated for node customer_trusted_join_accelerometer_trusted
customer_trusted_join_accelerometer_trusted_node2 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometer_landing_node1676651697036,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="customer_trusted_join_accelerometer_trusted_node2",
)

# Script generated for node filter_timestamp
SqlQuery0 = """
select * from myDataSource
where timestamp >= sharewithresearchasofdate
"""
filter_timestamp_node1676666861076 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": customer_trusted_join_accelerometer_trusted_node2},
    transformation_ctx="filter_timestamp_node1676666861076",
)

# Script generated for node drop_accelerometer_fields
drop_accelerometer_fields_node1676651872310 = DropFields.apply(
    frame=filter_timestamp_node1676666861076,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="drop_accelerometer_fields_node1676651872310",
)

# Script generated for node customer_curated
customer_curated_node3 = glueContext.getSink(
    path="s3://mlp-stedi-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node3",
)
customer_curated_node3.setCatalogInfo(
    catalogDatabase="mlp-stedi-db", catalogTableName="customer_curated"
)
customer_curated_node3.setFormat("json")
customer_curated_node3.writeFrame(drop_accelerometer_fields_node1676651872310)
job.commit()
