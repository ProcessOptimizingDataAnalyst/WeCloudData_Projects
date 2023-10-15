from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# NOTE: This variable needs to be reviewed if we are working with a new MSK
BOOTSTRAP_SERVERS='b-3.projecttraffic.pwajb3.c2.kafka.us-east-2.amazonaws.com:9092,b-1.projecttraffic.pwajb3.c2.kafka.us-east-2.amazonaws.com:9092,b-2.projecttraffic.pwajb3.c2.kafka.us-east-2.amazonaws.com:9092'
#BOOTSTRAP_SERVERS='localhost:9092'

if __name__ == "__main__":
   spark = SparkSession.builder.getOrCreate()

   # NOTE: we cant load the schema file from the local machine anymore, so we have to pull it from s3
   schema = spark.read.json('s3a://project-bus-status-b7/artifacts/bus_status_schema.json').schema

 # We have to connect to the bootstrap servers, instead of kafka:9092
   df = spark \
       .readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
       .option("subscribe", "dbserver1.demo.bus_status") \
       .option("startingOffsets", "latest") \
       .load()

   transform_df = df.select(col("value").cast("string")).alias("value").withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.payload.after.*")

   # NOTE: We cannot checkpoint to a local machine because we are working on the cloud. S3 is a reliable location for the cluster
   checkpoint_location = "s3a://project-bus-status-b7/msk/checkpoints"

   table_name = 'bus_status'
   hudi_options = {
       'hoodie.table.name': table_name,
       "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
       'hoodie.datasource.write.recordkey.field': 'record_id',
       'hoodie.datasource.write.partitionpath.field': 'routeId',
       'hoodie.datasource.write.table.name': table_name,
       'hoodie.datasource.write.operation': 'upsert',
       'hoodie.datasource.write.precombine.field': 'event_time',
      'hoodie.upsert.shuffle.parallelism': 100,
       'hoodie.insert.shuffle.parallelism': 100
   }

   s3_path = "s3a://project-bus-status-b7/msk/output"
  # s3_path = '/home/ec2-user/project_dependencies/output'

   def write_batch(batch_df, batch_id):
       batch_df.write.format("org.apache.hudi") \
       .options(**hudi_options) \
       .mode("append") \
       .save(s3_path)

   transform_df.writeStream.option("checkpointLocation", checkpoint_location).queryName("wcd-bus-streaming").foreachBatch(write_batch).start().awaitTermination()
  
