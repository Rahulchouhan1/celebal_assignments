# 🚀 Personal Data Migration & File Format Conversion (PySpark + Databricks)

This project performs a complete ETL pipeline using PySpark in Databricks. It involves:
- Loading raw CSV data
- Cleaning column names
- Writing to multiple formats: CSV, Parquet, Avro
- Saving to a MySQL database
- Copying entire tables between personal and portfolio databases

---

## 🛠️ Technologies Used
- **Apache Spark (PySpark)**
- **Databricks Community Edition**
- **MySQL (via JDBC)**
- **File Formats:** CSV, Parquet, Avro

---

## ✅ Step-by-Step Code Documentation

### 📥 Step 1: Export Data from Database as Raw CSV and Load into DBFS(databricks file system)

## ✅ Step 2: Clean Column Names

**🔍 Purpose:**  
Ensure compatibility with file formats and databases by replacing **spaces** (`" "`) and **dots** (`"."`) in column names with **underscores** (`"_"`).


## ✅ Step 3: Write to Multiple File Formats (CSV, Parquet, Avro)

**🔍 Purpose:**  
Save the cleaned dataset in multiple formats — **CSV**, **Parquet**, and **Avro** — to ensure flexibility and compatibility in downstream data processing, analytics, and storage systems.

## ✅ Step 4: Write Cleaned Data to MySQL (Portfolio DB)

**🔍 Purpose:**  
Store the cleaned data in a **MySQL portfolio database** to enable **structured querying**, **dashboarding**, or **integration with applications**.

%md
## ✅ Step 5: Copy Multiple Tables from Personal DB to  my Portfolio DB

# 📁 Project Structure

Project Root/
├── Data_pipeline_code   # PySpark and ETL pipeline scripts  
├── sample_data       # Input CSV or mock data files  
├── manifest.mf           # Metadata or config file  
└── readme                # Project overview or instructions



## 👨‍💻 Author

**Rahul Singh Chouhan** 

- 🔗 [GitHub](https://github.com/Rahulchouhan1)  
- 🔗 [LinkedIn](https://www.linkedin.com/in/rahul-singh-chouhan-5a7303270/)
