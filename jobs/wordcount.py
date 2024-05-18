import logging

from pyspark import SparkContext
from pyspark.sql import SparkSession
from core import wordcount_processor

logging.getLogger(__name__)

APP_NAME = "wordcount_app"
MASTER = "local[2]"

if __name__ == '__main__':
    spark = SparkSession.builder.master(MASTER).appName(APP_NAME).getOrCreate()
    sc: SparkContext = spark.sparkContext
    sc.setLogLevel("INFO")
    print(spark.version)

    wordcount_processor.run(spark, "/Users/rami/lab/zero/de-prep-project/jobs/wordcount.py",
                            "wc_output")

    spark.stop()
