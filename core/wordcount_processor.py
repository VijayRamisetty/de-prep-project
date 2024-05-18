import logging

from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("reading file from %s", input_path)
    input_df = spark.read.text(input_path)

    input_df = input_df.withColumn("value", regexp_replace(col('value'), '[^a-zA-Z\' \n]', " "))
    input_df.printSchema()
    input_df.show(100, truncate=False)
    logging.info("reading file from %s", output_path)

    result_rdd: RDD = (
        input_df.rdd.flatMap(lambda l: str(l[0]).split(" "))
        .filter(lambda l: len(str(l)) > 0)
        .map(lambda x: (str(x).lower(), 1))
        .reduceByKey(lambda x, y: x + y)
        .sortByKey()
    )
    for wc in result_rdd.collect():
        print(wc)
    result_df = spark.createDataFrame(result_rdd, ["word", "count"])
    result_df.show()

    result_df.coalesce(1).write.csv(output_path, header=True)
