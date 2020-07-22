import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#from awsglue.transforms import Relationalize

glueContext = GlueContext(spark.sparkContext)

glue_temp_storage = "s3://glue-etl-test-sriram/temp-dir"
glue_relationalize_output_s3_path = "s3://glue-etl-test-sriram/output-dir"
dfc_root_table_name = "joined_root"

output_temp_dir = "s3://glue-etl-test-sriram/temp-dir/"
db_name = "cfn-sriram-db1"
tbl_elements = "cfn_tbl_linkedin_email_raw_elements_json"
tbl_recipients = "cfn_tbl_linkedin_email_raw_recipient_json"

elements = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_elements)
recipients = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_recipients)

joined = Join.apply(elements, recipients, 'recipients', 'recipient_id').drop_fields(['recipient_id'])
dfc = joined.relationalize("joined_root", output_temp_dir)

for df_name in dfc.keys():
    m_df = dfc.select(df_name)
    elementsdataoutput = glueContext.write_dynamic_frame.from_options(frame = m_df, connection_type = "s3", connection_options = {"path": glue_relationalize_output_s3_path}, format = "parquet", transformation_ctx = "elementsdataoutput")
