from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from oeadbprphy.config.ConfigStore import *
from oeadbprphy.udfs.UDFs import *

def datasetoe(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", "jdbc:mysql://greensky57migrationrds.cqss4ijtzasl.ap-south-1.rds.amazonaws.com:3306/ovaledgedb")\
        .option("user", f"admin")\
        .option("password", f"0valEdge!")\
        .option("dbtable", "businessglossary")\
        .option("pushDownPredicate", True)\
        .option("driver", "com.mysql.jdbc.Driver")\
        .load()
