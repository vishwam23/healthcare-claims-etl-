# Databricks notebook source
# DBTITLE 1,Bronze Layer - Healthcare Claims
# MAGIC %md
# MAGIC # Bronze Layer - Healthcare Claims
# MAGIC
# MAGIC Load raw CSV data into Delta tables without transformations.

# COMMAND ----------

# DBTITLE 1,Create Bronze Schema and Volume
# MAGIC %sql
# MAGIC -- Create bronze schema and volume in healthcare catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare.bronze_data;
# MAGIC CREATE VOLUME IF NOT EXISTS healthcare.bronze_data.bronze;

# COMMAND ----------

# DBTITLE 1,Define source and target paths
# Define paths
raw_path = "/Volumes/healthcare/raw_data/raw"
bronze_path = "/Volumes/healthcare/bronze_data/bronze"

# List of tables to process
tables = ['claim', 'member', 'provider']

# COMMAND ----------

# DBTITLE 1,Load Claim data to Bronze
# Read claim CSV and write to bronze
claim_df = spark.read.csv(
    f"{raw_path}/claim.csv",
    header=True,
    inferSchema=True
)

# Cache to avoid re-scanning for count and display
claim_df.cache()
row_count = claim_df.count()

# Write to bronze as Delta table
claim_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/claim")

print(f"Claim data loaded to bronze. Row count: {row_count}")
display(claim_df.limit(5))

# Clean up cache
claim_df.unpersist()

# COMMAND ----------

# DBTITLE 1,Load Member data to Bronze
# Read member CSV and write to bronze
member_df = spark.read.csv(
    f"{raw_path}/member.csv",
    header=True,
    inferSchema=True
)

# Cache to avoid re-scanning for count and display
member_df.cache()
row_count = member_df.count()

# Write to bronze as Delta table
member_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/member")

print(f"Member data loaded to bronze. Row count: {row_count}")
display(member_df.limit(5))

# Clean up cache
member_df.unpersist()

# COMMAND ----------

# DBTITLE 1,Load Provider data to Bronze
# Read provider CSV and write to bronze
provider_df = spark.read.csv(
    f"{raw_path}/provider.csv",
    header=True,
    inferSchema=True
)

# Cache to avoid re-scanning for count and display
provider_df.cache()
row_count = provider_df.count()

# Write to bronze as Delta table
provider_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/provider")

print(f"Provider data loaded to bronze. Row count: {row_count}")
display(provider_df.limit(5))

# Clean up cache
provider_df.unpersist()

# COMMAND ----------

# DBTITLE 1,Summary of Bronze Layer
# Summary of bronze tables
print("\n=== Bronze Layer Summary ===")
for table in tables:
    df = spark.read.format("delta").load(f"{bronze_path}/{table}")
    print(f"{table.upper()}: {df.count()} rows, {len(df.columns)} columns")
    print(f"Columns: {', '.join(df.columns)}")
    print("-" * 50)

# COMMAND ----------

