from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from oeadbprphy.config.ConfigStore import *
from oeadbprphy.udfs.UDFs import *
from prophecy.utils import *
from oeadbprphy.graph import *

def pipeline(spark: SparkSession) -> None:
    df_datasetoe = datasetoe(spark)
    df_Filter_1 = Filter_1(spark, df_datasetoe)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/OE-ADB-PRPHY")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/OE-ADB-PRPHY")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
