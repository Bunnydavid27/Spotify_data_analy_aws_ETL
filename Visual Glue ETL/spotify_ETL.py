import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Albums
Albums_node1705153014501 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotifydataanaly2024/staging/Albums_cleaned.csv"],
        "recurse": True,
    },
    transformation_ctx="Albums_node1705153014501",
)

# Script generated for node Artists
Artists_node1705153016453 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotifydataanaly2024/staging/Artists_cleaned.csv"],
        "recurse": True,
    },
    transformation_ctx="Artists_node1705153016453",
)

# Script generated for node Tracks
Tracks_node1705153017036 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotifydataanaly2024/staging/Tracks_Cleaned.csv"],
        "recurse": True,
    },
    transformation_ctx="Tracks_node1705153017036",
)

# Script generated for node Join_Album_Artist
Albums_node1705153014501DF = Albums_node1705153014501.toDF()
Artists_node1705153016453DF = Artists_node1705153016453.toDF()
Join_Album_Artist_node1705153311705 = DynamicFrame.fromDF(
    Albums_node1705153014501DF.join(
        Artists_node1705153016453DF,
        (Albums_node1705153014501DF["artist_id"] == Artists_node1705153016453DF["id"]),
        "outer",
    ),
    glueContext,
    "Join_Album_Artist_node1705153311705",
)

# Script generated for node Join_TRack_AlbArt
Join_Album_Artist_node1705153311705DF = Join_Album_Artist_node1705153311705.toDF()
Tracks_node1705153017036DF = Tracks_node1705153017036.toDF()
Join_TRack_AlbArt_node1705153598458 = DynamicFrame.fromDF(
    Join_Album_Artist_node1705153311705DF.join(
        Tracks_node1705153017036DF,
        (
            Join_Album_Artist_node1705153311705DF["track_id"]
            == Tracks_node1705153017036DF["track_id"]
        ),
        "outer",
    ),
    glueContext,
    "Join_TRack_AlbArt_node1705153598458",
)

# Script generated for node Drop Fields
DropFields_node1705238727216 = DropFields.apply(
    frame=Join_TRack_AlbArt_node1705153598458,
    paths=["id"],
    transformation_ctx="DropFields_node1705238727216",
)

# Script generated for node Data Lake Destination
DataLakeDestination_node1705239126673 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1705238727216,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotifydataanaly2024/datawarehouse/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="DataLakeDestination_node1705239126673",
)

job.commit()
