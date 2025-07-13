
# Assignment : NYC Taxi Data Analysis using PySpark

### 🔹 Objective:
Load NYC Yellow Taxi Trip Data into **Azure Data Lake / Blob Storage / Databricks**, extract it into a **PySpark DataFrame**, Perform Following **Queries using PySpark**. 

**Query 1.** - Add a column named as ""`Revenue`"" into dataframe which is the sum of the below columns '`Fare_amount`','`Extra`','`MTA_tax`','`Improvement_surcharge`','`Tip_amount`','`Tolls_amount`','`Total_amount`' 

**Query 2.** - Increasing count of total passengers in New York City by area 

**Query 3.** - Realtime Average fare/total earning amount earned by 2 vendors 

**Query 4.** - Moving Count of payments made by each payment mode 

**Query 5.** - Highest two gaining vendor's on a particular date with no of passenger and total distance by cab 

**Query 6.** - Most no of passenger between a route of two location. 

**Query 7.** - Get top pickup locations with most passengers in last 5/10 seconds."


---

##  Dataset Source

| Dataset | Format | Link |
|--------|--------|------|
| Yellow Taxi Trip Data - January 2018 | Parquet | [yellow_tripdata_2018-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-01.parquet) |

---

##  Step 1: Setup & Data Load

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder     .appName("NYC Taxi Data Analysis")     .getOrCreate()

df = spark.read.option("header", "true")     .option("inferSchema", "true")     .parquet("/mnt/nyc-taxi/yellow_tripdata_2018-01.parquet")
```

---

##  Step 2: Queries

###  Query 1: Add a `Revenue` Column

```python
from pyspark.sql.functions import col

df = df.withColumn("Revenue", 
    col("fare_amount") + col("extra") + col("mta_tax") +
    col("improvement_surcharge") + col("tip_amount") +
    col("tolls_amount") + col("total_amount")
)

df.select("Revenue").show(5)
```

---

### ✅ Query 2: Total Passengers by Pickup Area (Increasing Count)

```python
df.groupBy("PULocationID")   .sum("passenger_count")   .withColumnRenamed("sum(passenger_count)", "total_passengers")   .orderBy("total_passengers", ascending=False)   .show()
```

---

### ✅ Query 3: Average Fare / Total Earnings by Vendor

```python
df.groupBy("VendorID")   .avg("total_amount")   .withColumnRenamed("avg(total_amount)", "average_earning")   .show()
```

---

### ✅ Query 4: Moving Count of Payments by Payment Mode

```python
df.groupBy("payment_type")   .count()   .withColumnRenamed("count", "payment_count")   .orderBy("payment_count", ascending=False)   .show()
```

---

### ✅ Query 5: Top 2 Earning Vendors on a Specific Date

```python
from pyspark.sql.functions import to_date

specific_date = "2018-01-15"
df_filtered = df.filter(to_date("tpep_pickup_datetime") == specific_date)

# Group by VendorID and aggregate
df_filtered.groupBy("VendorID")   .agg({
    "total_amount": "sum",
    "passenger_count": "sum",
    "trip_distance": "sum"
  })   .withColumnRenamed("sum(total_amount)", "total_earning")   .withColumnRenamed("sum(passenger_count)", "total_passengers")   .withColumnRenamed("sum(trip_distance)", "total_distance")   .orderBy("total_earning", ascending=False)   .show(2)
```

---

### ✅ Query 6: Route with Most Passengers

```python
df.groupBy("PULocationID", "DOLocationID")   .sum("passenger_count")   .withColumnRenamed("sum(passenger_count)", "total_passengers")   .orderBy("total_passengers", ascending=False)   .show(1)
```

---

### ✅ Query 7: Top Pickup Locations in Last 5/10 Seconds

```python
from pyspark.sql.functions import unix_timestamp

df = df.withColumn("pickup_unix", unix_timestamp("tpep_pickup_datetime"))

latest_time = df.select("pickup_unix").orderBy("pickup_unix", ascending=False).first()[0]


df.filter(col("pickup_unix") > (latest_time - 10))   .groupBy("PULocationID")   .sum("passenger_count")   .withColumnRenamed("sum(passenger_count)", "total_passengers")   .orderBy("total_passengers", ascending=False)   .show()
```

---



## 👨‍💻 Author

**Rahul Singh Chouhan**  
[LinkedIn](https://www.linkedin.com/in/rahul-singh-chouhan-5a7303270/)
