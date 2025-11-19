from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract, element_at, split, lpad, concat

# --- 1. Initialize Spark ---
spark = SparkSession.builder.appName("ConsolidateEPALogs").getOrCreate()

# --- 2. Load All JSON Files at Once ---
raw_df = spark.read.json("data/raw/*.json") \
    .withColumn("source_file", input_file_name())

# --- 3. Extract City and Pollutant from the Filename ---
# First, get just the filename (e.g., "Washington D.C._NO2.json")
df_with_filename = raw_df.withColumn("filename", element_at(split(col("source_file"), "/"), -1))
structured_df = df_with_filename.withColumn(
    "city", 
    regexp_extract(col("filename"), r"^(.*?)_([^_]+)\.json$", 1)
).withColumn(
    "pollutant",
    regexp_extract(col("filename"), r"^(.*?)_([^_]+)\.json$", 2)
)

# --- 4. Prepare for GDP Merge (from our previous conversation) ---
epa_data_ready = structured_df.withColumn("GeoFips",
    concat(
        lpad(col("state_code"), 2, '0'),
        lpad(col("county_code"), 3, '0')
    )
)

# --- 5. Clean Up and Final Result ---
final_df = epa_data_ready.drop("source_file", "filename")
print("DataFrame Schema:")
final_df.printSchema()

print("Example Data with City and Pollutant:")
final_df.select("city", "pollutant", "GeoFips", "year", "arithmetic_mean").show(5)

# --- 6. Write to Curated Output ---
final_df.write.mode("overwrite").parquet("data/curated/all_epa_data")

# --- 7. Join with GDP (Working on) ---