from pyspark.sql import SparkSession
import os

class SparkSingleton:
    """A singleton class on Datalib which returns one Spark instance"""
    __instance = None

    @classmethod
    def get_instance(cls):
        """Create a Spark instance for Datalib.
        :return: A Spark instance
        """
        # if cls.__instance is None:
        #     cls.__instance = (
        #         SparkSession.builder
        #             .config("spark.databricks.service.client.enabled", "true")
        #             .appName("datalib")
        #             .getOrCreate())
        #
        #     cls.__instance.sparkContext._jsc.hadoopConfiguration().set(
        #         "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        #     cls.__instance.sparkContext._jsc.hadoopConfiguration().set(
        #         "fs.s3a.endpoint", "s3.ca-central-1.amazonaws.com")
        #     cls.__instance.sparkContext._jsc.hadoopConfiguration().set(
        #         "fs.s3a.server-side-encryption-algorithm", "AES256")
        #     cls.__instance.sparkContext._jsc.hadoopConfiguration().set(
        #         "avro.mapred.ignore.inputs.without.extension", "false")
        #     cls.__instance.sparkContext.setSystemProperty(
        #         "com.amazonaws.services.s3.enableV4", "true")

        # return cls.__instance

        return (SparkSession.builder
                .getOrCreate())

    @classmethod
    def get_local_instance(cls):

        return (SparkSession.builder
                .master("local[*]")
                .appName("datalib")
                .getOrCreate())