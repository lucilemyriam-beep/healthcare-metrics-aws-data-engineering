import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1791997695123 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "multiLine": "true", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://healthcare-metrics-datalake-593271843946/bronze/PBJ_Daily_Nurse_Staffing_Q2_2024_UTF8.csv"]}, transformation_ctx="AmazonS3_node1791997695123")

# Script generated for node Change Schema
ChangeSchema_node1791998435296 = ApplyMapping.apply(frame=AmazonS3_node1791997695123, mappings=[("provnum", "string", "provnum", "string"), ("provname", "string", "provname", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("county_name", "string", "county_name", "string"), ("county_fips", "string", "county_fips", "string"), ("cy_qtr", "string", "cy_qtr", "string"), ("workdate", "string", "workdate", "string"), ("mdscensus", "string", "mdscensus", "string"), ("hrs_rndon", "string", "hrs_rndon", "string"), ("hrs_rndon_emp", "string", "hrs_rndon_emp", "string"), ("hrs_rndon_ctr", "string", "hrs_rndon_ctr", "string"), ("hrs_rnadmin", "string", "hrs_rnadmin", "string"), ("hrs_rnadmin_emp", "string", "hrs_rnadmin_emp", "string"), ("hrs_rnadmin_ctr", "string", "hrs_rnadmin_ctr", "string"), ("hrs_rn", "string", "hrs_rn", "string"), ("hrs_rn_emp", "string", "hrs_rn_emp", "string"), ("hrs_rn_ctr", "string", "hrs_rn_ctr", "string"), ("hrs_lpnadmin", "string", "hrs_lpnadmin", "string"), ("hrs_lpnadmin_emp", "string", "hrs_lpnadmin_emp", "string"), ("hrs_lpnadmin_ctr", "string", "hrs_lpnadmin_ctr", "string"), ("hrs_lpn", "string", "hrs_lpn", "string"), ("hrs_lpn_emp", "string", "hrs_lpn_emp", "string"), ("hrs_lpn_ctr", "string", "hrs_lpn_ctr", "string"), ("hrs_cna", "string", "hrs_cna", "string"), ("hrs_cna_emp", "string", "hrs_cna_emp", "string"), ("hrs_cna_ctr", "string", "hrs_cna_ctr", "string"), ("hrs_natrn", "string", "hrs_natrn", "string"), ("hrs_natrn_emp", "string", "hrs_natrn_emp", "string"), ("hrs_natrn_ctr", "string", "hrs_natrn_ctr", "string"), ("hrs_medaide", "string", "hrs_medaide", "string"), ("hrs_medaide_emp", "string", "hrs_medaide_emp", "string"), ("hrs_medaide_ctr", "string", "hrs_medaide_ctr", "string")], transformation_ctx="ChangeSchema_node1791998435296")

# Script generated for node Amazon S3
AmazonS3_node1792000065158 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1791998435296, connection_type="s3", format="glueparquet", connection_options={"path": "s3://healthcare-metrics-datalake-593271843946/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1792000065158")

job.commit()