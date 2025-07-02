# Databricks notebook source
# MAGIC %md
# MAGIC ## ✅ Step 1: Load Raw CSV Data
# MAGIC
# MAGIC **Purpose:** Load personal CSV file (e.g., contacts or projects) into a PySpark DataFrame.

# COMMAND ----------


sample_df = spark.read.option("header", True).csv("dbfs:/FileStore/Json_file_data1/json_df/_delta_log/project_raw_data.csv")
sample_df.show(5)
sample_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Step 2: Clean Column Names
# MAGIC
# MAGIC **🔍 Purpose:**  
# MAGIC Ensure compatibility with file formats and databases by replacing **spaces** (`" "`) and **dots** (`"."`) in column names with **underscores** (`"_"`).

# COMMAND ----------

from pyspark.sql.functions import col


cleaned_df = sample_df.select([
    col(c).alias(
        c.strip()              # remove leading/trailing spaces
         .replace(" ", "_")    # replace spaces with underscore
         .replace(".", "_")    # optional: replace dots
    ) for c in sample_df.columns
])


cleaned_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Step 3: Write to Multiple File Formats (CSV, Parquet, Avro)
# MAGIC
# MAGIC **🔍 Purpose:**  
# MAGIC Save the cleaned dataset in multiple formats — **CSV**, **Parquet**, and **Avro** — to ensure flexibility and compatibility in downstream data processing, analytics, and storage systems.
# MAGIC

# COMMAND ----------

# Write to CSV
cleaned_df.write.mode("overwrite").option("header", "true").csv("/FileStore/output/employees_csv")

# Write to Parquet
cleaned_df.write.mode("overwrite").parquet("/FileStore/output/employees_parquet")

# Write to Avro (requires package)
cleaned_df.write.mode("overwrite").format("avro").save("/FileStore/output/employees_avro")


# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Step 4: Write Cleaned Data to MySQL (Portfolio DB)
# MAGIC
# MAGIC **🔍 Purpose:**  
# MAGIC Store the cleaned data in a **MySQL portfolio database** to enable **structured querying**, **dashboarding**, or **integration with applications**.
# MAGIC

# COMMAND ----------

target_jdbc_url = "jdbc:mysql://localhost:3306/prakash_portfolio_db"

target_connection_properties = {
    "user": "prakash_user",
    "password": "prakash123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

cleaned_df.write.jdbc(
    url=target_jdbc_url,
    table="cleaned_contacts",
    mode="overwrite",
    properties=target_connection_properties
)

print("✅ Cleaned data written to prakash_portfolio_db.cleaned_contacts")



# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Step 5: Copy Multiple Tables from Personal DB to  my Portfolio DB
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

source_jdbc_url = "jdbc:mysql://localhost:3306/prakash_personal_db"
target_jdbc_url = "jdbc:mysql://localhost:3306/prakash_portfolio_db"

db_properties = {
    "user": "prakash_user",
    "password": "prakash123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

tables = ["contacts", "projects", "education"]

for table in tables:
    print(f"📤 Copying table: {table}")
    df = spark.read.jdbc(url=source_jdbc_url, table=table, properties=db_properties)
    df.write.jdbc(url=target_jdbc_url, table=table, mode="overwrite", properties=db_properties)

print("✅ All personal tables copied to prakash_portfolio_db")


# COMMAND ----------



# COMMAND ----------

