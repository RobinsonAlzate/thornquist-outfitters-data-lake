import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_flatten
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql("(select * from source1) UNION " + unionType + " (select * from source2)")
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

# Script generated for node Raw_POS_Sales
Raw_POS_Sales_node1792187076762 = glueContext.create_dynamic_frame.from_catalog(database="thornquist_db", table_name="pos", transformation_ctx="Raw_POS_Sales_node1792187076762")

# Script generated for node Raw_Ecommerce_Orders
Raw_Ecommerce_Orders_node1792187131391 = glueContext.create_dynamic_frame.from_catalog(database="thornquist_db", table_name="ecommerce", transformation_ctx="Raw_Ecommerce_Orders_node1792187131391")

# Script generated for node Raw_Legacy_Customers
Raw_Legacy_Customers_node1792187154414 = glueContext.create_dynamic_frame.from_catalog(database="thornquist_db", table_name="legacy_customers", transformation_ctx="Raw_Legacy_Customers_node1792187154414")

# Script generated for node Change Schema
ChangeSchema_node1792187349088 = ApplyMapping.apply(frame=Raw_POS_Sales_node1792187076762, mappings=[("order_id", "string", "order_id", "string"), ("sale_date", "string", "sale_date", "string"), ("customer_id", "string", "customer_id", "string"), ("product_id", "string", "product_id", "string"), ("product_name", "string", "product_name", "string"), ("category", "string", "category", "string"), ("region", "string", "province", "string"), ("currency", "string", "currency", "string"), ("unit_price", "double", "unit_price", "double"), ("quantity", "long", "quantity", "long")], transformation_ctx="ChangeSchema_node1792187349088")

# Script generated for node Flatten
Flatten_node1792187997190 = Raw_Ecommerce_Orders_node1792187131391.gs_flatten()

# Script generated for node Change Schema
ChangeSchema_node1792190776184 = ApplyMapping.apply(frame=Raw_Legacy_Customers_node1792187154414, mappings=[("col0", "string", "customer_id", "string"), ("col1", "string", "customer_name", "string"), ("col2", "string", "province", "string"), ("col3", "string", "segment", "string"), ("col4", "string", "join_date", "string")], transformation_ctx="ChangeSchema_node1792190776184")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1792188544978 = ApplyMapping.apply(frame=ChangeSchema_node1792187349088, mappings=[("order_id", "string", "right_order_id", "string"), ("sale_date", "string", "right_sale_date", "string"), ("customer_id", "string", "right_customer_id", "string"), ("product_id", "string", "right_product_id", "string"), ("product_name", "string", "right_product_name", "string"), ("category", "string", "right_category", "string"), ("province", "string", "province", "string"), ("currency", "string", "right_currency", "string"), ("unit_price", "double", "right_unit_price", "double"), ("quantity", "long", "right_quantity", "long")], transformation_ctx="RenamedkeysforJoin_node1792188544978")

# Script generated for node Change Schema
ChangeSchema_node1792187413912 = ApplyMapping.apply(frame=Flatten_node1792187997190, mappings=[("orderNumber", "string", "order_id", "string"), ("orderDate", "string", "sale_date", "string"), ("customerId", "string", "customer_id", "string"), ("sku", "string", "product_id", "string"), ("product", "string", "product_name", "string"), ("productGroup", "string", "category", "string"), ("province", "string", "province", "string"), ("currency", "string", "currency", "string"), ("price", "double", "unit_price", "double"), ("qty", "int", "quantity", "int")], transformation_ctx="ChangeSchema_node1792187413912")

# Script generated for node Union
Union_node1792189001174 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node1792187413912, "source2": ChangeSchema_node1792187349088}, transformation_ctx = "Union_node1792189001174")

# Script generated for node Trusted_Customers_Target
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1792190776184, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792184651085", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Trusted_Customers_Target_node1792190901911 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1792190776184, connection_type="s3", format="glueparquet", connection_options={"path": "s3://thornquist-outfitters-robinson/trusted/customers/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Trusted_Customers_Target_node1792190901911")

# Script generated for node Curated_Sales_Target
EvaluateDataQuality().process_rows(frame=Union_node1792189001174, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792195076553", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Curated_Sales_Target_node1792196504692 = glueContext.write_dynamic_frame.from_options(frame=Union_node1792189001174, connection_type="s3", format="glueparquet", connection_options={"path": "s3://thornquist-outfitters-robinson/curated/conformed_sales/", "partitionKeys": ["category"]}, format_options={"compression": "snappy"}, transformation_ctx="Curated_Sales_Target_node1792196504692")

# Script generated for node Trusted_S3_Target
EvaluateDataQuality().process_rows(frame=Union_node1792189001174, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792184651085", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Trusted_S3_Target_node1792188400280 = glueContext.write_dynamic_frame.from_options(frame=Union_node1792189001174, connection_type="s3", format="json", connection_options={"path": "s3://thornquist-outfitters-robinson/trusted/conformed_sales/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="Trusted_S3_Target_node1792188400280")

job.commit()