from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def main(s3_file_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataValidationJob").getOrCreate()

    # Define schema for CSV data
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_time", TimestampType(), True),
        StructField("delivery_time", TimestampType(), True),
        StructField("order_value", DoubleType(), True),
        # Add more fields as needed
    ])

    # Read CSV data from S3 with specified schema
    df = spark.read.csv(s3_file_path, schema=schema, header=True)

    # Validate order_time and delivery_time are in correct datetime format
    # Ensure order_value is positive
    df_validated = df.filter(
        (df.order_time.isNotNull()) &
        (df.delivery_time.isNotNull()) &
        (df.order_value > 0)
    )

    # Transform data as needed (e.g., add derived columns, perform aggregations)
    # Example: Calculate delivery time difference in minutes
    df_transformed = df_validated.withColumn(
        "delivery_time_diff_minutes",
        (df_validated.delivery_time - df_validated.order_time).cast("long") / 60
    )

    # Save the validated and transformed data back to S3 or process it further
    # Example: Save as Parquet file back to S3
    output_s3_path = s3_file_path.replace(".csv", "_transformed.parquet")
    df_transformed.write.parquet(output_s3_path)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    import sys
    # Get the S3 file path from the command-line argument
    s3_file_path = sys.argv[1]
    main(s3_file_path)




low_threshold = 50
    high_threshold = 150
    
    # Add a new column 'order_value_category' based on order_value
    df_transformed = df_validated.withColumn(
        "order_value_category",
        when(col("order_value") < low_threshold, "Low")
        .when((col("order_value") >= low_threshold) & (col("order_value") <= high_threshold), "Medium")
        .otherwise("High")
    )

    # Process the data further as needed
    # Example: Save the transformed data back to S3 as a Parquet file
    output_s3_path = s3_file_path.replace(".csv", "_transformed.parquet")
    df_transformed.write.parquet(output_s3_path)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    import sys
    # Get the S3 file path from the command-line argument
    s3_file_path = sys.argv[1]
    main(s3_file_path)



    # Write transformed data to Redshift
    df_transformed.write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshift_url) \
        .option("user", redshift_user) \
        .option("password", redshift_password) \
        .option("dbtable", "your_staging_table")  # The staging table in Redshift
        .option("tempdir", temp_s3_path)  # Temporary data path in S3
        .mode("append")  # You can choose "append" or "overwrite" based on your use case
        .save()

    # Stop the Spark session
    spark.stop()