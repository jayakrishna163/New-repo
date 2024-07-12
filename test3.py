# Databricks notebook source
# Import necessary libraries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window
from pyspark import SparkConf  # Import SparkConf

# Define ETLProcessor class
class ETLProcessor:
    def __init__(self):
        try:
            self.spark = SparkSession.builder \
                .appName("ETL Job Test") \
                .getOrCreate()
        except Exception as e:
            print(f"Error initializing SparkSession: {str(e)}")

    def log_quality_control_metrics(self, table_name, metric_name, metric_flag, metric_numeric, sample_data, as_of_date, is_anomaly):
        schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_flag", StringType(), True),
            StructField("metric_numeric", IntegerType(), True),
            StructField("sample_data", StringType(), True),
            StructField("as_of_date", DateType(), True),
            StructField("is_anomaly", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        quality_control_data = [{
            'table_name': table_name,
            'metric_name': metric_name,
            'metric_flag': metric_flag,
            'metric_numeric': metric_numeric,
            'sample_data': sample_data,
            'as_of_date': as_of_date,
            'is_anomaly': is_anomaly,
            'update_ts': datetime.datetime.now()
        }]

        try:
            quality_control_df = self.spark.createDataFrame(quality_control_data, schema)
            quality_control_df.show()

            # Write to Hive table using Databricks-specific connector
            quality_control_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("etl_test.managed_quality_control_table")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, end_time):
        try:
            # Define anomaly detection logic here
            anomaly_query = """
                SELECT * FROM etl_test.managed_quality_control_table
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum')
                AND table_name = '{}'
            """.format(rpt_table)

            # Read anomaly metrics from Hive
            anomaly_metrics = self.spark.sql(anomaly_query)

            # Define window specification to get the latest update_ts for each metric_name
            window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())

            # Filter to get the latest metrics from managed_quality_control_table
            latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                            .filter(col("rank") == 1) \
                                            .drop("rank")

            # Define a mapping for metric names to their corresponding values
            metrics_mapping = {
                "row_count": rows_inserted_count,
                "metric1_sum": metric1_sum,
                "metric2_sum": metric2_sum
            }

            # Prepare to collect anomaly data for logging
            anomaly_data = []

            # Compare metrics and identify anomalies for current run
            for row in latest_metrics.collect():
                metric_name = row["metric_name"]
                metric_numeric = row["metric_numeric"]

                if metric_name in metrics_mapping:
                    etl_value = metrics_mapping[metric_name]

                    # Determine if there is an anomaly
                    is_anomaly = "No"

                    if etl_value == 0:
                        is_anomaly = "Yes"

                    # Calculate 50% more and 50% less of metric_numeric (previous value)
                    fifty_percent_more = metric_numeric * 1.5
                    fifty_percent_less = metric_numeric * 0.5

                    if etl_value > fifty_percent_more:
                        is_anomaly = "Yes"
                    if etl_value < fifty_percent_less:
                        is_anomaly = "Yes"

                    # Log quality control metrics
                    self.log_quality_control_metrics(rpt_table, metric_name, "numeric", etl_value, None, end_time.date(), is_anomaly)

        except Exception as e:
            print(f"Error in anomaly detection: {str(e)}")



# Main ETL script
if __name__ == "__main__":
    # Configure Spark driver and executor memory
    spark_conf = SparkConf()
    spark_conf.set("spark.driver.memory", "4g")
    spark_conf.set("spark.executor.memory", "4g")

    # Initialize Spark session with configurations
    spark = SparkSession.builder \
        .appName("ETL Job Test") \
        .config(conf=spark_conf) \
        .enableHiveSupport() \
        .getOrCreate()

    # Database and table names
    hive_database = "etl_test"
    acl_table = f"{hive_database}.acl3"
    rpt_table = f"{hive_database}.rpt2"
    process_name = "etl-job_test"

    # Initialize ETLProcessor instance
    etl_processor = ETLProcessor()

    try:
        # Read data from acl table
        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        # Transform data: subtract 2 from the age column to create age_2years_ago
        acl_df.createOrReplaceTempView("acl_temp")
        acl_df.show()

        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM acl_temp
        """)

        rpt_df.show()

        # Write transformed data to rpt table
        rpt_df.write.mode("append").saveAsTable(rpt_table)

        # Calculate row count and sum of metric1 and metric2
        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg({"metric1": "sum"}).collect()[0][0]
        sum_metrics2 = rpt_df.agg({"metric2": "sum"}).collect()[0][0]

        # Call detect_anomalies to show latest_metrics
        end_time = datetime.datetime.now()
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, end_time)

    finally:
        # Close the ETLProcessor and Spark session
        etl_processor.close()
        spark.stop()

# COMMAND ----------

# Import necessary libraries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window
from pyspark import SparkConf  # Import SparkConf

# Define ETLProcessor class
class ETLProcessor:
    def __init__(self):
        try:
            self.spark = SparkSession.builder \
                .appName("ETL Job Test") \
                .getOrCreate()
        except Exception as e:
            print(f"Error initializing SparkSession: {str(e)}")

    def log_quality_control_metrics(self, table_name, metric_name, metric_flag, metric_numeric, sample_data, as_of_date, is_anomaly):
        schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_flag", StringType(), True),
            StructField("metric_numeric", IntegerType(), True),
            StructField("sample_data", StringType(), True),
            StructField("as_of_date", DateType(), True),
            StructField("is_anomaly", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        quality_control_data = [{
            'table_name': table_name,
            'metric_name': metric_name,
            'metric_flag': metric_flag,
            'metric_numeric': metric_numeric,
            'sample_data': sample_data,
            'as_of_date': as_of_date,
            'is_anomaly': is_anomaly,
            'update_ts': datetime.datetime.now()
        }]

        try:
            quality_control_df = self.spark.createDataFrame(quality_control_data, schema)
            quality_control_df.show()

            # Write to Hive table using Databricks-specific connector
            quality_control_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("etl_test.managed_quality_control_table")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, end_time):
        try:
            # Define anomaly detection logic here
            anomaly_query = """
                SELECT * FROM etl_test.managed_quality_control_table
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum')
                AND table_name = '{}'
            """.format(rpt_table)

            # Read anomaly metrics from Hive
            anomaly_metrics = self.spark.sql(anomaly_query)

            # Define window specification to get the latest update_ts for each metric_name
            window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())

            # Filter to get the latest metrics from managed_quality_control_table
            latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                            .filter(col("rank") == 1) \
                                            .drop("rank")

            # Define a mapping for metric names to their corresponding values
            metrics_mapping = {
                "row_count": rows_inserted_count,
                "metric1_sum": metric1_sum,
                "metric2_sum": metric2_sum
            }

            # Prepare to collect anomaly data for logging
            anomaly_data = []

            # Compare metrics and identify anomalies for current run
            for row in latest_metrics.collect():
                metric_name = row["metric_name"]
                metric_numeric = row["metric_numeric"]

                if metric_name in metrics_mapping:
                    etl_value = metrics_mapping[metric_name]

                    # Determine if there is an anomaly
                    is_anomaly = "No"

                    if etl_value == 0:
                        is_anomaly = "Yes"

                    # Calculate 50% more and 50% less of metric_numeric (previous value)
                    fifty_percent_more = metric_numeric * 1.5
                    fifty_percent_less = metric_numeric * 0.5

                    if etl_value > fifty_percent_more:
                        is_anomaly = "Yes"
                    if (etl_value < fifty_percent_less):
                        is_anomaly = "Yes"

                    # Log quality control metrics
                    self.log_quality_control_metrics(rpt_table, metric_name, "numeric", etl_value, None, end_time.date(), is_anomaly)

        except Exception as e:
            print(f"Error in anomaly detection: {str(e)}")

    def close(self):
        try:
            self.spark.stop()
        except Exception as e:
            print(f"Error closing Spark session: {str(e)}")


# Main ETL script
if __name__ == "__main__":
    # Configure Spark driver and executor memory
    spark_conf = SparkConf()
    spark_conf.set("spark.driver.memory", "4g")
    spark_conf.set("spark.executor.memory", "4g")

    # Initialize Spark session with configurations
    spark = SparkSession.builder \
        .appName("ETL Job Test") \
        .config(conf=spark_conf) \
        .enableHiveSupport() \
        .getOrCreate()

    # Database and table names
    hive_database = "etl_test"
    acl_table = f"{hive_database}.acl3"
    rpt_table = f"{hive_database}.rpt2"
    process_name = "etl-job_test"

    # Initialize ETLProcessor instance
    etl_processor = ETLProcessor()

    try:
        # Read data from acl table
        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        # Transform data: subtract 2 from the age column to create age_2years_ago
        acl_df.createOrReplaceTempView("acl_temp")
        acl_df.show()

        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM acl_temp
        """)

        rpt_df.show()

        # Write transformed data to rpt table
        rpt_df.write.mode("append").saveAsTable(rpt_table)

        # Calculate row count and sum of metric1 and metric2
        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg({"metric1": "sum"}).collect()[0][0]
        sum_metrics2 = rpt_df.agg({"metric2": "sum"}).collect()[0][0]

        # Call detect_anomalies to show latest_metrics
        end_time = datetime.datetime.now()
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, end_time)

    finally:
        # Close the ETLProcessor and Spark session
        etl_processor.close()
        spark.stop()


# COMMAND ----------


