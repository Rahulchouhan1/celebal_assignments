# 🚖 NYC Taxi Data Analysis using PySpark

# -----------------------------------------------
# Step 1: Environment Setup & Data Loading
# -----------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Data Analysis") \
    .getOrCreate()

# Load dataset from Azure/Blob/ADLS (adjust path if needed)
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .parquet("/mnt/nyc-taxi/yellow_tripdata_2018-01.parquet")

# -----------------------------------------------
# Query 1: Add a Revenue Column
# -----------------------------------------------

df = df.withColumn("Revenue", 
    col("fare_amount") + col("extra") + col("mta_tax") +
    col("improvement_surcharge") + col("tip_amount") +
    col("tolls_amount") + col("total_amount")
)

df.select("Revenue").show(5)

# -----------------------------------------------
# Query 2: Total Passengers by Pickup Area
# -----------------------------------------------

df.groupBy("PULocationID") \
  .sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False) \
  .show()

# -----------------------------------------------
# Query 3: Average Fare / Earnings by Vendor
# -----------------------------------------------

df.groupBy("VendorID") \
  .avg("total_amount") \
  .withColumnRenamed("avg(total_amount)", "average_earning") \
  .show()

# -----------------------------------------------
# Query 4: Moving Count of Payments by Payment Mode
# -----------------------------------------------

df.groupBy("payment_type") \
  .count() \
  .withColumnRenamed("count", "payment_count") \
  .orderBy("payment_count", ascending=False) \
  .show()

# -----------------------------------------------
# Query 5: Top 2 Earning Vendors on Specific Date
# -----------------------------------------------

specific_date = "2018-01-15"

df_filtered = df.filter(to_date("tpep_pickup_datetime") == specific_date)

df_filtered.groupBy("VendorID") \
  .agg({
    "total_amount": "sum",
    "passenger_count": "sum",
    "trip_distance": "sum"
  }) \
  .withColumnRenamed("sum(total_amount)", "total_earning") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .withColumnRenamed("sum(trip_distance)", "total_distance") \
  .orderBy("total_earning", ascending=False) \
  .show(2)

# -----------------------------------------------
# Query 6: Most Passengers Between Two Locations
# -----------------------------------------------

df.groupBy("PULocationID", "DOLocationID") \
  .sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False) \
  .show(1)

# -----------------------------------------------
# Query 7: Top Pickup Locations in Last 10 Seconds
# -----------------------------------------------

df = df.withColumn("pickup_unix", unix_timestamp("tpep_pickup_datetime"))

latest_time = df.select("pickup_unix") \
    .orderBy("pickup_unix", ascending=False) \
    .first()[0]

df.filter(col("pickup_unix") > (latest_time - 10)) \
  .groupBy("PULocationID") \
  .sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False) \
  .show()

# -----------------------------------------------
# ✅ End of Analysis
# -----------------------------------------------

print("✅ NYC Taxi Data Analysis Completed.")

🧑‍💻 Author
Rahul Singh Chouhan
[LinkedIn](https://www.linkedin.com/in/rahul-singh-chouhan-5a7303270/)
