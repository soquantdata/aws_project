import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session## @type: DataSource
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


output_path = "s3://<bucket>/glue-demo/reportsales/"
# @type: DataSource
# @args: [database = "glue-demo", table_name = "dimproduct", transformation_ctx0 = "datasource0"]
# @return: datasource0
# @inputs: []


datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue-demo", table_name = "dimproduct", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("productid", "long", "productid", "long"), ("division", "string", "division", "string"), ("gender", "string", "gender", "string"), ("category", "string", "category", "string")], transformation_ctx = "applymapping0"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping0 = ApplyMapping.apply(frame = datasource0, mappings = [("productid", "long", "productid", "long"), ("division", "string", "division", "string"), ("gender", "string", "gender", "string"), ("category", "string", "category", "string")], transformation_ctx = "applymapping0")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": output_path, "compression": "gzip"}, format = "csv", transformation_ctx = "datasink0"]
## @return: datasink0
## @inputs: [frame = applymapping0]


datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "glue-demo", table_name = "dimstore", transformation_ctx = "datasource1")
## @type: ApplyMapping
## @args: [mapping = [("storeid", "long", "storeid", "long"), ("channel", "string", "channel", "string"), ("country", "string", "country", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource1, mappings = [("storeid", "long", "storeid", "long"), ("channel", "string", "channel", "string"), ("country", "string", "country", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": output_path, "compression": "gzip"}, format = "csv", transformation_ctx = "datasink1"]
## @return: datasink1
## @inputs: [frame = applymapping1]

datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "glue-demo", table_name = "dimdate", transformation_ctx = "datasource2")
## @type: ApplyMapping
## @args: [mapping = [("datekey", "long", "datekey", "long"), ("datecalendarday", "long", "datecalendarday", "long"), ("datecalendaryear", "long", "datecalendaryear", "long"), ("weeknumberofseason", "long", "weeknumberofseason", "long")], transformation_ctx = "applymapping2"]
## @return: applymapping1
## @inputs: [frame = datasource2]
applymapping2 = ApplyMapping.apply(frame = datasource2, mappings = [("datekey", "long", "datekey", "long"), ("datecalendarday", "long", "datecalendarday", "long"), ("datecalendaryear", "long", "datecalendaryear", "long"), ("weeknumberofseason", "long", "weeknumberofseason", "long")], transformation_ctx = "applymapping2")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": output_path, "compression": "gzip"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping2]

datasource3 = glueContext.create_dynamic_frame.from_catalog(database = "glue-demo", table_name = "factsales", transformation_ctx = "datasource3")
## @type: ApplyMapping
## @args: [mapping = [("saleid", "long", "saleid", "long"), ("netsales", "double", "netsales", "double"), ("salesunits", "long", "salesunits", "long"), ("storeid", "long", "storeid", "long"), ("dateid", "long", "dateid", "long"), ("productid", "long", "productid", "long")], transformation_ctx = "applymapping3"]
## @return: applymapping3
## @inputs: [frame = datasource3]
applymapping3 = ApplyMapping.apply(frame = datasource3, mappings = [("saleid", "long", "saleid", "long"), ("netsales", "double", "netsales", "double"), ("salesunits", "long", "salesunits", "long"), ("storeid", "long", "storeid", "long"), ("dateid", "long", "dateid", "long"), ("productid", "long", "productid", "long")], transformation_ctx = "applymapping3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": output_path, "compression": "gzip"}, format = "csv", transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = applymapping3]

applymapping0.toDF().createOrReplaceTempView("view_product")
applymapping1.toDF().createOrReplaceTempView("view_store")
applymapping2.toDF().createOrReplaceTempView("view_date")
applymapping3.toDF().createOrReplaceTempView("view_sales")

finalsql = """Select concat(CAST(dd.datecalendaryear as varchar(20)),'_', 
                       CAST(ds.channel as varchar(20)), '_', CAST(dp.division as varchar(20)), '_', CAST(dp.gender as varchar(20))
                       , '_', CAST(dp.category as varchar(20))) as uniquekey, fs.dateid, fs.storeid, fs.productid, fs.saleid, fs.netsales, fs.salesunits ,
                dd.datecalendaryear, dd.weeknumberofseason,
                ds.channel, ds.country,
                dp.division, dp.gender, dp.category
                from view_sales fs 
                JOIN view_date dd on dd.datekey = fs.dateid
                JOIN view_store ds on ds.storeid = fs.storeid
                JOIN view_product dp on dp.productid = fs.productid
            """

salesDF = spark.sql(finalsql)
# salesDF.show()


# finaldf = pd.DataFrame(columns = ['uniqueKey', 'channel', 'division', 'country', 'category', 'datecalendaryear', 'weeknumberofseason', 'netsales', 'salesunits'])

finaldf = salesDF.groupby(
                ['uniqueKey', 'channel', 'division', 'country', 'category', 'datecalendaryear', 'weeknumberofseason']
                ).agg(
                    {
                    'netsales': 'sum',
                    'salesunits': 'sum'
                    }
                )



salesDDF2 = DynamicFrame.fromDF(finaldf, glueContext , "salesDDF2")
              
applymapping4 = ApplyMapping.apply(frame = salesDDF2, mappings = [("uniqueKey", "string", "uniqueKey", "string"), ("channel", "string", "channel", "string"), ("division", "string", "division", "string")
                ,("country", "string", "country", "string"), ("category", "string", "category", "string"), ("datecalendaryear", "long", "datecalendaryear", "long")
                ,("weeknumberofseason", "long", "weeknumberofseason", "long"), ("sum(salesunits)", "long", "netsales", "long"), ("sum(netsales)", "double", "salesunits", "double") ], transformation_ctx = "applymapping4")


# To export as csv
# datasink3 = glueContext.write_dynamic_frame.from_options(frame = applymapping4, connection_type = "s3", connection_options = {"path": "s3://<bucketname>/glue-demo/reportsales", "compression": "gzip"}, format = "csv", transformation_ctx = "datasink3")

#To export as parquet  part file
# datasink3 = glueContext.write_dynamic_frame.from_options(frame = applymapping4, connection_type = "s3", connection_options = {"path": output_path }, format = "parquet", transformation_ctx = "datasink3")

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
applymapping4.toDF().write.mode("overwrite").format("parquet").partitionBy("uniqueKey", "weeknumberofseason").save(output_path)


job.commit()