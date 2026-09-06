# Databricks notebook source
# DBTITLE 1,Gold Layer - Business Aggregations
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations
# MAGIC
# MAGIC Create business-ready aggregated tables for analytics and reporting from clean Silver layer data.

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql import functions as F

# Paths
silver_path = '/Volumes/healthcare/silver_data/silver'
gold_path = '/Volumes/healthcare/gold_data/gold'

print("✓ Setup complete")
print(f"Silver: {silver_path}")
print(f"Gold: {gold_path}")

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
# MAGIC %sql
# MAGIC -- Create gold schema and volume
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare.gold_data;
# MAGIC CREATE VOLUME IF NOT EXISTS healthcare.gold_data.gold;
# MAGIC
# MAGIC SELECT '✓ Gold schema ready' as status;

# COMMAND ----------

# DBTITLE 1,Claims by Provider Summary
# Read clean data from Silver
claim_df = spark.read.format("delta").load(f"{silver_path}/claim")
provider_df = spark.read.format("delta").load(f"{silver_path}/provider")

# Aggregate: Claims by Provider
provider_summary = claim_df.join(
    provider_df,
    'provider_id',
    'inner'
).groupBy(
    'provider_id',
    'provider_name',
    'specialty',
    'state'
).agg(
    F.count('claim_id').alias('total_claims'),
    F.sum('claim_amount').alias('total_amount'),
    F.avg('claim_amount').alias('avg_claim_amount'),
    F.countDistinct('member_id').alias('unique_members')
).orderBy(F.desc('total_amount'))

# Write to Gold
provider_summary.write.format("delta").mode("overwrite").save(f"{gold_path}/provider_summary")

print(f"✓ Provider Summary created: {provider_summary.count()} records")
display(provider_summary.limit(10))

# COMMAND ----------

# DBTITLE 1,Member Claims Summary
# Read clean data from Silver
member_df = spark.read.format("delta").load(f"{silver_path}/member")

# Aggregate: Claims by Member
member_summary = claim_df.join(
    member_df,
    'member_id',
    'inner'
).groupBy(
    'member_id',
    'member_name',
    'plan_type',
    'state'
).agg(
    F.count('claim_id').alias('total_claims'),
    F.sum('claim_amount').alias('total_amount'),
    F.avg('claim_amount').alias('avg_claim_amount'),
    F.min('service_date').alias('first_claim_date'),
    F.max('service_date').alias('last_claim_date')
).orderBy(F.desc('total_amount'))

# Write to Gold
member_summary.write.format("delta").mode("overwrite").save(f"{gold_path}/member_summary")

print(f"✓ Member Summary created: {member_summary.count()} records")
display(member_summary.limit(10))

# COMMAND ----------

# DBTITLE 1,Monthly Trends
# Aggregate: Monthly Claim Trends
monthly_trends = claim_df.withColumn(
    'claim_month',
    F.date_format('service_date', 'yyyy-MM')
).groupBy(
    'claim_month'
).agg(
    F.count('claim_id').alias('total_claims'),
    F.sum('claim_amount').alias('total_amount'),
    F.avg('claim_amount').alias('avg_claim_amount'),
    F.countDistinct('member_id').alias('unique_members'),
    F.countDistinct('provider_id').alias('unique_providers')
).orderBy('claim_month')

# Write to Gold
monthly_trends.write.format("delta").mode("overwrite").save(f"{gold_path}/monthly_trends")

print(f"✓ Monthly Trends created: {monthly_trends.count()} records")
display(monthly_trends)

# COMMAND ----------

# DBTITLE 1,Diagnosis Code Analysis
# Aggregate: Top Diagnosis Codes
diagnosis_summary = claim_df.groupBy(
    'diagnosis_code'
).agg(
    F.count('claim_id').alias('total_claims'),
    F.sum('claim_amount').alias('total_amount'),
    F.avg('claim_amount').alias('avg_claim_amount'),
    F.countDistinct('member_id').alias('unique_members')
).orderBy(F.desc('total_claims'))

# Write to Gold
diagnosis_summary.write.format("delta").mode("overwrite").save(f"{gold_path}/diagnosis_summary")

print(f"✓ Diagnosis Summary created: {diagnosis_summary.count()} records")
display(diagnosis_summary.limit(10))

# COMMAND ----------

# DBTITLE 1,Gold Layer Summary
# Summary of all Gold tables
print("=" * 60)
print("GOLD LAYER SUMMARY")
print("=" * 60)

gold_tables = [
    'provider_summary',
    'member_summary',
    'monthly_trends',
    'diagnosis_summary'
]

for table in gold_tables:
    try:
        df = spark.read.format("delta").load(f"{gold_path}/{table}")
        print(f"\n{table}: {df.count():,} records")
    except Exception as e:
        print(f"\n{table}: Not found or error")

print("\n" + "=" * 60)
print("✓ Gold layer aggregations complete")
print(f"✓ Data saved to: {gold_path}")
print("=" * 60)

# COMMAND ----------

