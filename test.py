# Databricks notebook source
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank, sum
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Job Test") \
    .enableHiveSupport() \
    .getOrCreate()

class ETLProcessor:
    def __init__(self, spark):
        self.spark = spark

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

            #quality_control_df.write.format("delta") \
             #   .mode("append") \
              #  .saveAsTable("etl_test.managed_quality_control_table")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, sample_data, end_time):
        try:
            anomaly_query = """
                SELECT * FROM log_test.managed_quality_control_table
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum', 'sample_data')
                AND table_name = '{}'
            """.format(rpt_table)

            anomaly_metrics = self.spark.sql(anomaly_query)

            if anomaly_metrics.count() == 0:
                self.log_quality_control_metrics(rpt_table, "row_count", "numeric", rows_inserted_count, None, end_time.date(), "Yes" if rows_inserted_count == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric1_sum", "numeric", metric1_sum, None, end_time.date(), "Yes" if metric1_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric2_sum", "numeric", metric2_sum, None, end_time.date(), "Yes" if metric2_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", len(sample_data) if sample_data else 0, sample_data, end_time.date(), "Yes" if not sample_data or len(sample_data) == 0 else "No")
            else:
                window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())
                latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                                .filter(col("rank") == 1) \
                                                .drop("rank")

                metrics_mapping = {
                    "row_count": rows_inserted_count,
                    "metric1_sum": metric1_sum,
                    "metric2_sum": metric2_sum,
                    "sample_data": sample_data
                }

                for row in latest_metrics.collect():
                    metric_name = row["metric_name"]
                    metric_flag = row["metric_flag"]
                    metric_numeric = row["metric_numeric"]
                    current_sample_data = row["sample_data"]

                    if metric_name in metrics_mapping:
                        etl_value = metrics_mapping[metric_name]
                        is_anomaly = "No"

                        if metric_flag == "numeric":
                            if etl_value == 0:
                                is_anomaly = "Yes"

                            fifty_percent_more = metric_numeric * 1.5
                            fifty_percent_less = metric_numeric * 0.5

                            if etl_value > fifty_percent_more:
                                is_anomaly = "Yes"
                            if etl_value < fifty_percent_less:
                                is_anomaly = "Yes"

                        elif metric_flag == "varchar":
                            if current_sample_data is None or len(current_sample_data) == 0:
                                is_anomaly = "Yes"

                        self.log_quality_control_metrics(rpt_table, metric_name, metric_flag, metric_numeric, current_sample_data, end_time.date(), is_anomaly)
        except Exception as e:
            print(f"Error in detect_anomalies: {e}")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    acl_table = "etl_test.acl3"
    rpt_table = "etl_test.rpt2"

    try:
        # Build Spark session for ETL job
        spark = SparkSession.builder \
            .appName("ETL Job") \
            .enableHiveSupport() \
            .getOrCreate()

        # Capture initial row count before the ETL process
        initial_row_count = spark.sql("SELECT COUNT(*) AS row_count FROM {}.{}".format(rpt_table)).collect()[0][0]

        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        acl_df.createOrReplaceTempView("rpt_table_temp")
        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM rpt_table_temp
        """)

        rpt_df.show()

        rpt_df.write.mode("append").saveAsTable(rpt_table)

        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg(sum(col("metric1"))).collect()[0][0]
        sum_metrics2 = rpt_df.agg(sum(col("metric2"))).collect()[0][0]

        sample_data = rpt_df.select("center_code").first()[0] if rpt_df.count() > 0 else None

        end_time = datetime.datetime.now()

        etl_processor = ETLProcessor(spark)
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, sample_data, end_time)

        spark.stop()
    except Exception as e:
        print(f"Error in main ETL process: {e}")

# COMMAND ----------

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank, sum
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Job Test") \
    .enableHiveSupport() \
    .getOrCreate()

class ETLProcessor:
    def __init__(self, spark):
        self.spark = spark

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

            quality_control_df.write.format("hive") \
                .mode("append") \
                .saveAsTable("etl_test.managed_quality_control_table")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, sample_data, end_time):
        try:
            anomaly_query = f"""
                SELECT * FROM etl_test.managed_quality_control_table
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum', 'sample_data')
                AND table_name = '{rpt_table}'
            """

            anomaly_metrics = self.spark.sql(anomaly_query)

            if anomaly_metrics.count() == 0:
                self.log_quality_control_metrics(rpt_table, "row_count", "numeric", rows_inserted_count, None, end_time.date(), "Yes" if rows_inserted_count == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric1_sum", "numeric", metric1_sum, None, end_time.date(), "Yes" if metric1_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric2_sum", "numeric", metric2_sum, None, end_time.date(), "Yes" if metric2_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", len(sample_data) if sample_data else 0, sample_data, end_time.date(), "Yes" if not sample_data or len(sample_data) == 0 else "No")
            else:
                window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())
                latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                                .filter(col("rank") == 1) \
                                                .drop("rank")

                metrics_mapping = {
                    "row_count": rows_inserted_count,
                    "metric1_sum": metric1_sum,
                    "metric2_sum": metric2_sum,
                    "sample_data": sample_data
                }

                for row in latest_metrics.collect():
                    metric_name = row["metric_name"]
                    metric_flag = row["metric_flag"]
                    metric_numeric = row["metric_numeric"]
                    current_sample_data = row["sample_data"]

                    if metric_name in metrics_mapping:
                        etl_value = metrics_mapping[metric_name]
                        is_anomaly = "No"

                        if metric_flag == "numeric":
                            if etl_value == 0:
                                is_anomaly = "Yes"

                            fifty_percent_more = metric_numeric * 1.5
                            fifty_percent_less = metric_numeric * 0.5

                            if etl_value > fifty_percent_more or etl_value < fifty_percent_less:
                                is_anomaly = "Yes"

                        elif metric_flag == "varchar":
                            if current_sample_data is None or len(current_sample_data) == 0:
                                is_anomaly = "Yes"

                        self.log_quality_control_metrics(rpt_table, metric_name, metric_flag, metric_numeric, current_sample_data, end_time.date(), is_anomaly)
        except Exception as e:
            print(f"Error in detect_anomalies: {e}")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    
    acl_table = "etl_test.acl3"
    rpt_table = "etl_test.rpt2"

    try:
        # Capture initial row count before the ETL process
        initial_row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {rpt_table}").collect()[0][0]

        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        acl_df.createOrReplaceTempView("rpt_table_temp")
        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM rpt_table_temp
        """)

        rpt_df.show()

        rpt_df.write.mode("append").saveAsTable(rpt_table)

        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg(sum(col("metric1"))).collect()[0][0]
        sum_metrics2 = rpt_df.agg(sum(col("metric2"))).collect()[0][0]

        sample_data = rpt_df.select("center_code").first()[0] if rpt_df.count() > 0 else None

        end_time = datetime.datetime.now()

        etl_processor = ETLProcessor(spark)
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, sample_data, end_time)

        spark.stop()
    except Exception as e:
        print(f"Error in main ETL process: {e}")


# COMMAND ----------



# COMMAND ----------

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank, sum, max as spark_max
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Job Test") \
    .enableHiveSupport() \
    .getOrCreate()

class ETLProcessor:
    def __init__(self, spark):
        self.spark = spark

    def get_next_row_num(self, table_name):
        row_num_query = f"""
            SELECT IFNULL(MAX(row_number), 0) + 1 AS next_row_num FROM etl_test.managed_quality_control_table
            WHERE table_name = '{table_name}'
        """
        next_row_num = self.spark.sql(row_num_query).collect()[0][0]
        return next_row_num

    def log_quality_control_metrics(self, table_name, metric_name, metric_flag, metric_numeric, sample_data, as_of_date, is_anomaly):
        schema = StructType([
            StructField("row_number", IntegerType(), True),
            StructField("table_name", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_flag", StringType(), True),
            StructField("metric_numeric", IntegerType(), True),
            StructField("sample_data", StringType(), True),
            StructField("as_of_date", DateType(), True),
            StructField("is_anomaly", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        # Fetch next row number
        next_row_num = self.get_next_row_num(table_name)

        quality_control_data = [{
            'row_number': next_row_num,
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

            quality_control_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("etl_test.managed_quality_control_table1")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, sample_data, end_time):
        try:
            anomaly_query = f"""
                SELECT * FROM etl_test.managed_quality_control_table
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum', 'sample_data')
                AND table_name = '{rpt_table}'
            """

            anomaly_metrics = self.spark.sql(anomaly_query)

            if anomaly_metrics.count() == 0:
                self.log_quality_control_metrics(rpt_table, "row_count", "numeric", rows_inserted_count, None, end_time.date(), "Yes" if rows_inserted_count == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric1_sum", "numeric", metric1_sum, None, end_time.date(), "Yes" if metric1_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric2_sum", "numeric", metric2_sum, None, end_time.date(), "Yes" if metric2_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", len(sample_data) if sample_data else 0, sample_data, end_time.date(), "Yes" if not sample_data or len(sample_data) == 0 else "No")
            else:
                window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())
                latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                                .filter(col("rank") == 1) \
                                                .drop("rank")

                metrics_mapping = {
                    "row_count": rows_inserted_count,
                    "metric1_sum": metric1_sum,
                    "metric2_sum": metric2_sum,
                    "sample_data": sample_data
                }

                for row in latest_metrics.collect():
                    metric_name = row["metric_name"]
                    metric_flag = row["metric_flag"]
                    metric_numeric = row["metric_numeric"]
                    current_sample_data = row["sample_data"]

                    if metric_name in metrics_mapping:
                        etl_value = metrics_mapping[metric_name]
                        is_anomaly = "No"

                        if metric_flag == "numeric":
                            if etl_value == 0:
                                is_anomaly = "Yes"

                            fifty_percent_more = metric_numeric * 1.5
                            fifty_percent_less = metric_numeric * 0.5

                            if etl_value > fifty_percent_more or etl_value < fifty_percent_less:
                                is_anomaly = "Yes"

                        elif metric_flag == "varchar":
                            if current_sample_data is None or len(current_sample_data) == 0:
                                is_anomaly = "Yes"

                        self.log_quality_control_metrics(rpt_table, metric_name, metric_flag, metric_numeric, current_sample_data, end_time.date(), is_anomaly)
        except Exception as e:
            print(f"Error in detect_anomalies: {e}")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    hive_database = "etl_test"
    acl_table = f"{hive_database}.acl3"
    rpt_table = f"{hive_database}.rpt2"

    try:
        # Verify and create the database if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database} LOCATION '/etl_test'")

        # Create acl3 table and insert values
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {acl_table} (
                name STRING,
                age INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create rpt2 table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {rpt_table} (
                name STRING,
                age INT,
                age_2years_ago INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create managed_quality_control_table if it does not exist
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {hive_database}.managed_quality_control_table1 (
                row_number INT,
                table_name STRING,
                metric_name STRING,
                metric_flag STRING,
                metric_numeric INT,
                sample_data STRING,
                as_of_date DATE,
                is_anomaly STRING,
                update_ts TIMESTAMP
            )
        """)

        # Capture initial row count before the ETL process
        initial_row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {rpt_table}").collect()[0][0]

        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        acl_df.createOrReplaceTempView("rpt_table_temp")
        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM rpt_table_temp
        """)

        rpt_df.show()

        rpt_df.write.mode("append").saveAsTable(rpt_table)

        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg(sum(col("metric1"))).collect()[0][0]
        sum_metrics2 = rpt_df.agg(sum(col("metric2"))).collect()[0][0]

        sample_data = rpt_df.select("name").first()[0] if rpt_df.count() > 0 else None

        end_time = datetime.datetime.now()

        etl_processor = ETLProcessor(spark)
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, sample_data, end_time)

    except Exception as e:
        print(f"Error in main ETL process: {e}")
    finally:
        spark.stop()


# COMMAND ----------

df= spark.sql("select * from etl_test.managed_quality_control_table")
df.show()

# COMMAND ----------

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank, sum
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Job Test") \
    .enableHiveSupport() \
    .getOrCreate()

class ETLProcessor:
    def __init__(self, spark):
        self.spark = spark

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

            quality_control_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("etl_test.managed_quality_control_table")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, sample_data, end_time):
        try:
            anomaly_query = f"""
                SELECT * FROM etl_test.managed_quality_control_table
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum', 'sample_data')
                AND table_name = '{rpt_table}'
            """

            anomaly_metrics = self.spark.sql(anomaly_query)

            if anomaly_metrics.count() == 0:
                self.log_quality_control_metrics(rpt_table, "row_count", "numeric", rows_inserted_count, None, end_time.date(), "Yes" if rows_inserted_count == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric1_sum", "numeric", metric1_sum, None, end_time.date(), "Yes" if metric1_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric2_sum", "numeric", metric2_sum, None, end_time.date(), "Yes" if metric2_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", len(sample_data) if sample_data else 0, sample_data, end_time.date(), "Yes" if not sample_data or len(sample_data) == 0 else "No")
            else:
                window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())
                latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                                .filter(col("rank") == 1) \
                                                .drop("rank")

                metrics_mapping = {
                    "row_count": rows_inserted_count,
                    "metric1_sum": metric1_sum,
                    "metric2_sum": metric2_sum,
                    "sample_data": sample_data
                }

                for row in latest_metrics.collect():
                    metric_name = row["metric_name"]
                    metric_flag = row["metric_flag"]
                    metric_numeric = row["metric_numeric"]
                    current_sample_data = row["sample_data"]

                    if metric_name in metrics_mapping:
                        etl_value = metrics_mapping[metric_name]
                        is_anomaly = "No"

                        if metric_flag == "numeric":
                            if etl_value == 0:
                                is_anomaly = "Yes"

                            fifty_percent_more = metric_numeric * 1.5
                            fifty_percent_less = metric_numeric * 0.5

                            if etl_value > fifty_percent_more or etl_value < fifty_percent_less:
                                is_anomaly = "Yes"

                        elif metric_flag == "varchar":
                            if current_sample_data is None or len(current_sample_data) == 0:
                                is_anomaly = "Yes"

                        self.log_quality_control_metrics(rpt_table, metric_name, metric_flag, metric_numeric, current_sample_data, end_time.date(), is_anomaly)
        except Exception as e:
            print(f"Error in detect_anomalies: {e}")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    hive_database = "etl_test"
    acl_table = f"{hive_database}.acl3"
    rpt_table = f"{hive_database}.rpt2"

    try:
        # Verify and create the database if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database} LOCATION '/etl_test'")

        # Create acl3 table and insert values
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {acl_table} (
                name STRING,
                age INT,
                metric1 INT,
                metric2 INT
            )
        """)


        # Create rpt2 table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {rpt_table} (
                name STRING,
                age INT,
                age_2years_ago INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create managed_quality_control_table if it does not exist
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {hive_database}.managed_quality_control_table (
                table_name STRING,
                metric_name STRING,
                metric_flag STRING,
                metric_numeric INT,
                sample_data STRING,
                as_of_date DATE,
                is_anomaly STRING,
                update_ts TIMESTAMP
            ) USING DELTA
        """)

        # Capture initial row count before the ETL process
        initial_row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {rpt_table}").collect()[0][0]

        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        acl_df.createOrReplaceTempView("rpt_table_temp")
        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM rpt_table_temp
        """)

        rpt_df.show()

        rpt_df.write.mode("append").saveAsTable(rpt_table)

        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg(sum(col("metric1"))).collect()[0][0]
        sum_metrics2 = rpt_df.agg(sum(col("metric2"))).collect()[0][0]

        sample_data = rpt_df.select("name").first()[0] if rpt_df.count() > 0 else None

        end_time = datetime.datetime.now()

        etl_processor = ETLProcessor(spark)
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, sample_data, end_time)
    except Exception as e:
        print(f"Error in main ETL process: {e}")
    finally:
        spark.stop()


# COMMAND ----------

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank, sum, max as spark_max
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Job Test") \
    .enableHiveSupport() \
    .getOrCreate()

class ETLProcessor:
    def __init__(self, spark):
        self.spark = spark

    def get_next_row_num(self, table_name):
        row_num_query = f"""
            SELECT IFNULL(MAX(row_number), 0) + 1 AS next_row_num FROM etl_test.managed_quality_control_table1
            WHERE table_name = '{table_name}'
        """
        next_row_num = self.spark.sql(row_num_query).collect()[0][0]
        return next_row_num

    def log_quality_control_metrics(self, table_name, metric_name, metric_flag, metric_numeric, sample_data, as_of_date, is_anomaly):
        schema = StructType([
            StructField("row_number", IntegerType(), True),
            StructField("table_name", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_flag", StringType(), True),
            StructField("metric_numeric", IntegerType(), True),
            StructField("sample_data", StringType(), True),
            StructField("as_of_date", DateType(), True),
            StructField("is_anomaly", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        # Fetch next row number
        next_row_num = self.get_next_row_num(table_name)

        quality_control_data = [{
            'row_number': next_row_num,
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

            quality_control_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("etl_test.managed_quality_control_table1")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, sample_data, end_time):
        try:
            anomaly_query = f"""
                SELECT * FROM etl_test.managed_quality_control_table1
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum', 'sample_data')
                AND table_name = '{rpt_table}'
            """

            anomaly_metrics = self.spark.sql(anomaly_query)

            if anomaly_metrics.count() == 0:
                self.log_quality_control_metrics(rpt_table, "row_count", "numeric", rows_inserted_count, None, end_time.date(), "Yes" if rows_inserted_count == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric1_sum", "numeric", metric1_sum, None, end_time.date(), "Yes" if metric1_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric2_sum", "numeric", metric2_sum, None, end_time.date(), "Yes" if metric2_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", len(sample_data) if sample_data else 0, sample_data, end_time.date(), "Yes" if not sample_data or len(sample_data) == 0 else "No")
            else:
                window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())
                latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                                .filter(col("rank") == 1) \
                                                .drop("rank")

                metrics_mapping = {
                    "row_count": rows_inserted_count,
                    "metric1_sum": metric1_sum,
                    "metric2_sum": metric2_sum,
                    "sample_data": sample_data
                }

                for row in latest_metrics.collect():
                    metric_name = row["metric_name"]
                    metric_flag = row["metric_flag"]
                    metric_numeric = row["metric_numeric"]
                    current_sample_data = row["sample_data"]

                    if metric_name in metrics_mapping:
                        etl_value = metrics_mapping[metric_name]
                        is_anomaly = "No"

                        if metric_flag == "numeric":
                            if etl_value == 0:
                                is_anomaly = "Yes"

                            fifty_percent_more = metric_numeric * 1.5
                            fifty_percent_less = metric_numeric * 0.5

                            if etl_value > fifty_percent_more or etl_value < fifty_percent_less:
                                is_anomaly = "Yes"

                        elif metric_flag == "varchar":
                            if current_sample_data is None or len(current_sample_data) == 0:
                                is_anomaly = "Yes"

                        self.log_quality_control_metrics(rpt_table, metric_name, metric_flag, etl_value, sample_data, end_time.date(), is_anomaly)
        except Exception as e:
            print(f"Error in detect_anomalies: {e}")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    hive_database = "etl_test"
    acl_table = f"{hive_database}.acl3"
    rpt_table = f"{hive_database}.rpt2"

    try:
        # Verify and create the database if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database} LOCATION '/etl_test'")

        # Create acl3 table and insert values
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {acl_table} (
                name STRING,
                age INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create rpt2 table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {rpt_table} (
                name STRING,
                age INT,
                age_2years_ago INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create managed_quality_control_table if it does not exist
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {hive_database}.managed_quality_control_table1 (
                row_number INT,
                table_name STRING,
                metric_name STRING,
                metric_flag STRING,
                metric_numeric INT,
                sample_data STRING,
                as_of_date DATE,
                is_anomaly STRING,
                update_ts TIMESTAMP
            )
        """)

        # Capture initial row count before the ETL process
        initial_row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {rpt_table}").collect()[0][0]

        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        acl_df.createOrReplaceTempView("rpt_table_temp")
        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM rpt_table_temp limit 10
        """)

        rpt_df.show()

        rpt_df.write.mode("append").saveAsTable(rpt_table)

        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg(sum(col("metric1"))).collect()[0][0]
        sum_metrics2 = rpt_df.agg(sum(col("metric2"))).collect()[0][0]

        sample_data = rpt_df.select("name").first()[0] if rpt_df.count() > 0 else None

        end_time = datetime.datetime.now()

        etl_processor = ETLProcessor(spark)
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, sample_data, end_time)

    except Exception as e:
        print(f"Error in main ETL process: {e}")
    finally:
        spark.stop()


# COMMAND ----------

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank, sum, max as spark_max
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Job Test") \
    .enableHiveSupport() \
    .getOrCreate()

class ETLProcessor:
    def __init__(self, spark):
        self.spark = spark

    def get_next_row_num(self, table_name):
        row_num_query = f"""
            SELECT IFNULL(MAX(row_number), 0) + 1 AS next_row_num FROM etl_test.managed_quality_control_table1
            WHERE table_name = '{table_name}'
        """
        next_row_num = self.spark.sql(row_num_query).collect()[0][0]
        return next_row_num

    def log_quality_control_metrics(self, table_name, metric_name, metric_flag, metric_numeric, sample_data, as_of_date, is_anomaly):
        schema = StructType([
            StructField("row_number", IntegerType(), True),
            StructField("table_name", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_flag", StringType(), True),
            StructField("metric_numeric", IntegerType(), True),
            StructField("sample_data", StringType(), True),
            StructField("as_of_date", DateType(), True),
            StructField("is_anomaly", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        # Fetch next row number
        next_row_num = self.get_next_row_num(table_name)

        quality_control_data = [{
            'row_number': next_row_num,
            'table_name': table_name,
            'metric_name': metric_name,
            'metric_flag': metric_flag,
            'metric_numeric': metric_numeric if metric_flag == 'numeric' else None,
            'sample_data': sample_data if metric_flag == 'varchar' else None,
            'as_of_date': as_of_date,
            'is_anomaly': is_anomaly,
            'update_ts': datetime.datetime.now()
        }]

        try:
            quality_control_df = self.spark.createDataFrame(quality_control_data, schema)
            quality_control_df.show()

            quality_control_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("etl_test.managed_quality_control_table1")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, sample_data, end_time):
        try:
            anomaly_query = f"""
                SELECT * FROM etl_test.managed_quality_control_table1
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum', 'sample_data')
                AND table_name = '{rpt_table}'
            """

            anomaly_metrics = self.spark.sql(anomaly_query)

            if anomaly_metrics.count() == 0:
                self.log_quality_control_metrics(rpt_table, "row_count", "numeric", rows_inserted_count, None, end_time.date(), "Yes" if rows_inserted_count == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric1_sum", "numeric", metric1_sum, None, end_time.date(), "Yes" if metric1_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric2_sum", "numeric", metric2_sum, None, end_time.date(), "Yes" if metric2_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", None, sample_data, end_time.date(), "Yes" if not sample_data or len(sample_data) == 0 else "No")
            else:
                window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())
                latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                                .filter(col("rank") == 1) \
                                                .drop("rank")

                metrics_mapping = {
                    "row_count": rows_inserted_count,
                    "metric1_sum": metric1_sum,
                    "metric2_sum": metric2_sum,
                    "sample_data": sample_data
                }

                for row in latest_metrics.collect():
                    metric_name = row["metric_name"]
                    metric_flag = row["metric_flag"]
                    metric_numeric = row["metric_numeric"]
                    current_sample_data = row["sample_data"]

                    if metric_name in metrics_mapping:
                        etl_value = metrics_mapping[metric_name]
                        is_anomaly = "No"

                        if metric_flag == "numeric":
                            if etl_value == 0:
                                is_anomaly = "Yes"

                            fifty_percent_more = metric_numeric * 1.5
                            fifty_percent_less = metric_numeric * 0.5

                            if etl_value > fifty_percent_more or etl_value < fifty_percent_less:
                                is_anomaly = "Yes"

                        elif metric_flag == "varchar":
                            if current_sample_data is None or len(current_sample_data) == 0:
                                is_anomaly = "Yes"

                        self.log_quality_control_metrics(rpt_table, metric_name, metric_flag, etl_value if metric_flag == "numeric" else None, etl_value if metric_flag == "varchar" else None, end_time.date(), is_anomaly)
        except Exception as e:
            print(f"Error in detect_anomalies: {e}")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    hive_database = "etl_test"
    acl_table = f"{hive_database}.acl3"
    rpt_table = f"{hive_database}.rpt2"

    try:
        # Verify and create the database if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database} LOCATION '/etl_test'")

        # Create acl3 table and insert values
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {acl_table} (
                name STRING,
                age INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create rpt2 table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {rpt_table} (
                name STRING,
                age INT,
                age_2years_ago INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create managed_quality_control_table if it does not exist
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {hive_database}.managed_quality_control_table1 (
                row_number INT,
                table_name STRING,
                metric_name STRING,
                metric_flag STRING,
                metric_numeric INT,
                sample_data STRING,
                as_of_date DATE,
                is_anomaly STRING,
                update_ts TIMESTAMP
            )
        """)

        # Capture initial row count before the ETL process
        initial_row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {rpt_table}").collect()[0][0]

        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        acl_df.createOrReplaceTempView("rpt_table_temp")
        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM rpt_table_temp limit 10
        """)

        rpt_df.show()

        rpt_df.write.mode("append").saveAsTable(rpt_table)

        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg(sum(col("metric1"))).collect()[0][0]
        sum_metrics2 = rpt_df.agg(sum(col("metric2"))).collect()[0][0]

        sample_data = rpt_df.select("name").first()[0] if rpt_df.count() > 0 else None

        end_time = datetime.datetime.now()

        etl_processor = ETLProcessor(spark)
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, sample_data, end_time)

    except Exception as e:
        print(f"Error in main ETL process: {e}")
    finally:
        spark.stop()


# COMMAND ----------

df = spark.sql("SELECT * FROM etl_test.managed_quality_control_table1")
df.show()

# COMMAND ----------

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.functions import col, dense_rank, sum as spark_sum
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Job Test") \
    .enableHiveSupport() \
    .getOrCreate()

class ETLProcessor:
    def __init__(self, spark):
        self.spark = spark

    def get_next_row_num(self, table_name):
        row_num_query = f"""
            SELECT IFNULL(MAX(row_number), 0) + 1 AS next_row_num FROM etl_test.managed_quality_control_table1
            WHERE table_name = '{table_name}'
        """
        next_row_num = self.spark.sql(row_num_query).collect()[0][0]
        return next_row_num

    def log_quality_control_metrics(self, table_name, metric_name, metric_flag, metric_numeric, sample_data, as_of_date, is_anomaly):
        schema = StructType([
            StructField("row_number", IntegerType(), True),
            StructField("table_name", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_flag", StringType(), True),
            StructField("metric_numeric", IntegerType(), True),
            StructField("sample_data", StringType(), True),
            StructField("as_of_date", DateType(), True),
            StructField("is_anomaly", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        # Fetch next row number
        next_row_num = self.get_next_row_num(table_name)

        # Calculate metric_numeric for varchar metrics
        if metric_flag == 'varchar':
            metric_numeric = len(sample_data) if sample_data else None

        quality_control_data = [{
            'row_number': next_row_num,
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

            quality_control_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("etl_test.managed_quality_control_table1")
        except Exception as e:
            print(f"Error creating DataFrame or writing to Hive table: {str(e)}")

    def detect_anomalies(self, rpt_table, rows_inserted_count, metric1_sum, metric2_sum, sample_data, end_time):
        try:
            anomaly_query = f"""
                SELECT * FROM etl_test.managed_quality_control_table1
                WHERE metric_name IN ('row_count', 'metric1_sum', 'metric2_sum', 'sample_data')
                AND table_name = '{rpt_table}'
            """

            anomaly_metrics = self.spark.sql(anomaly_query)

            if anomaly_metrics.count() == 0:
                self.log_quality_control_metrics(rpt_table, "row_count", "numeric", rows_inserted_count, None, end_time.date(), "Yes" if rows_inserted_count == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric1_sum", "numeric", metric1_sum, None, end_time.date(), "Yes" if metric1_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric2_sum", "numeric", metric2_sum, None, end_time.date(), "Yes" if metric2_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", len(sample_data) if sample_data else 0, sample_data, end_time.date(), "Yes" if not sample_data or len(sample_data) == 0 else "No")
            else:
                window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())
                latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                                .filter(col("rank") == 1) \
                                                .drop("rank")

                metrics_mapping = {
                    "row_count": rows_inserted_count,
                    "metric1_sum": metric1_sum,
                    "metric2_sum": metric2_sum,
                    "sample_data": sample_data 
                }

                for row in latest_metrics.collect():
                    metric_name = row["metric_name"]
                    metric_flag = row["metric_flag"]
                    metric_numeric = row["metric_numeric"] 
                    current_sample_data = row["sample_data"]

                    if metric_name in metrics_mapping:
                        etl_value = metrics_mapping[metric_name]
                        is_anomaly = "No"

                        if metric_flag == "numeric":
                            if etl_value == 0:
                                is_anomaly = "Yes"

                            fifty_percent_more = metric_numeric * 1.5
                            fifty_percent_less = metric_numeric * 0.5

                            if etl_value > fifty_percent_more or etl_value < fifty_percent_less:
                                is_anomaly = "Yes"

                        elif metric_flag == "varchar":
                            if current_sample_data is None:
                                is_anomaly = "Yes"
                            elif len(current_sample_data) == 0:
                                is_anomaly = "Yes"

                        self.log_quality_control_metrics(rpt_table, metric_name, metric_flag, etl_value if metric_flag == "numeric" else None, etl_value if metric_flag == "varchar" else None, end_time.date(), is_anomaly)
        except Exception as e:
            print(f"Error in detect_anomalies: {e}")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    hive_database = "etl_test"
    acl_table = f"{hive_database}.acl3"
    rpt_table = f"{hive_database}.rpt2"

    try:
        # Verify and create the database if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database} LOCATION '/etl_test'")

        # Create acl3 table and insert values
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {acl_table} (
                name STRING,
                age INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create rpt2 table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {rpt_table} (
                name STRING,
                age INT,
                age_2years_ago INT,
                metric1 INT,
                metric2 INT
            )
        """)

        # Create managed_quality_control_table if it does not exist
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {hive_database}.managed_quality_control_table1 (
                row_number INT,
                table_name STRING,
                metric_name STRING,
                metric_flag STRING,
                metric_numeric INT,
                sample_data STRING,
                as_of_date DATE,
                is_anomaly STRING,
                update_ts TIMESTAMP
            )
        """)

        # Capture initial row count before the ETL process
        initial_row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {rpt_table}").collect()[0][0]

        acl_df = spark.sql(f"SELECT * FROM {acl_table}")

        acl_df.createOrReplaceTempView("rpt_table_temp")
        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
            FROM rpt_table_temp limit 4
        """)

        rpt_df.show()

        rpt_df.write.mode("append").saveAsTable(rpt_table)

        rows_inserted_count = rpt_df.count()
        sum_metrics1 = rpt_df.agg(spark_sum(col("metric1"))).collect()[0][0]
        sum_metrics2 = rpt_df.agg(spark_sum(col("metric2"))).collect()[0][0]

        sample_data = rpt_df.select("name").first()[0] if rpt_df.count() > 0 else None

        end_time = datetime.datetime.now()

        etl_processor = ETLProcessor(spark)
        etl_processor.detect_anomalies(rpt_table, rows_inserted_count, sum_metrics1, sum_metrics2, sample_data, end_time)

    except Exception as e:
        print(f"Error in main ETL process: {e}")
    finally:
        spark.stop()


# COMMAND ----------

df = spark.sql("truncate table etl_test.managed_quality_control_table1")

# COMMAND ----------


