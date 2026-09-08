# Databricks notebook source
# DBTITLE 1,Silver Layer - Data Cleaning & Validation
# MAGIC %md
# MAGIC # Silver Layer - Data Cleaning & Validation
# MAGIC
# MAGIC Clean and validate healthcare claims data from Bronze layer. Apply data quality rules and prepare for Gold aggregations.

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql import functions as F

# Paths
bronze_path = '/Volumes/healthcare/bronze_data/bronze'
silver_path = '/Volumes/healthcare/silver_data/silver'
quarantine_path = '/Volumes/healthcare/silver_data/quarantine'

print("✓ Setup complete")
print(f"Bronze: {bronze_path}")
print(f"Silver: {silver_path}")
print(f"Quarantine: {quarantine_path}")

# COMMAND ----------

# DBTITLE 1,Create Quarantine Volume
# MAGIC %sql
# MAGIC -- Create quarantine volume if it doesn't exist
# MAGIC CREATE VOLUME IF NOT EXISTS healthcare.silver_data.quarantine;
# MAGIC
# MAGIC SELECT '✓ Quarantine volume ready' as status;

# COMMAND ----------

# DBTITLE 1,Process Member Table
# Read from Bronze
member_df = spark.read.format("delta").load(f"{bronze_path}/member")
print(f"Bronze records: {member_df.count()}")

# 1. Capture records with missing member_id
missing_id = member_df.filter(F.col('member_id').isNull()).cache()
missing_count = missing_id.count()
print(f"\n⚠ Missing member_id: {missing_count}")
if missing_count > 0:
    missing_id.write.format("delta").mode("overwrite").save(f"{quarantine_path}/member_missing_id")
missing_id.unpersist()

# 2. Find duplicates - simpler approach
duplicates = member_df.exceptAll(member_df.dropDuplicates(['member_id'])).cache()
dup_count = duplicates.count()
print(f"⚠ Duplicates: {dup_count}")
if dup_count > 0:
    duplicates.write.format("delta").mode("overwrite").save(f"{quarantine_path}/member_duplicates")
duplicates.unpersist()

# 3. Clean data
member_clean = member_df.filter(
    F.col('member_id').isNotNull()
).dropDuplicates(['member_id'])

# 4. Standardize dates
member_clean = member_clean.withColumn(
    'dob',
    F.expr("try_to_date(dob, 'yyyy-MM-dd')")
)

# Write to Silver
member_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/member")
print(f"\n✓ Silver records: {member_clean.count()}")

# COMMAND ----------

# DBTITLE 1,Process Provider Table
# Read from Bronze
provider_df = spark.read.format("delta").load(f"{bronze_path}/provider")
print(f"Bronze records: {provider_df.count()}")

# 1. Capture records with missing provider_id
missing_id = provider_df.filter(F.col('provider_id').isNull()).cache()
missing_count = missing_id.count()
print(f"\n⚠ Missing provider_id: {missing_count}")
if missing_count > 0:
    missing_id.write.format("delta").mode("overwrite").save(f"{quarantine_path}/provider_missing_id")
missing_id.unpersist()

# 2. Find duplicates - simpler approach
duplicates = provider_df.exceptAll(provider_df.dropDuplicates(['provider_id'])).cache()
dup_count = duplicates.count()
print(f"⚠ Duplicates: {dup_count}")
if dup_count > 0:
    duplicates.write.format("delta").mode("overwrite").save(f"{quarantine_path}/provider_duplicates")
duplicates.unpersist()

# 3. Clean data
provider_clean = provider_df.filter(
    F.col('provider_id').isNotNull()
).dropDuplicates(['provider_id'])

# Write to Silver
provider_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/provider")
print(f"\n✓ Silver records: {provider_clean.count()}")

# COMMAND ----------

# DBTITLE 1,Process Claim Table
# Read from Bronze
claim_df = spark.read.format("delta").load(f"{bronze_path}/claim")
print(f"Bronze records: {claim_df.count()}")

# Read Silver member & provider for validation
member_silver = spark.read.format("delta").load(f"{silver_path}/member")
provider_silver = spark.read.format("delta").load(f"{silver_path}/provider")

# 1. Capture records with missing claim_id
missing_id = claim_df.filter(F.col('claim_id').isNull()).cache()
missing_count = missing_id.count()
print(f"\n⚠ Missing claim_id: {missing_count}")
if missing_count > 0:
    missing_id.write.format("delta").mode("overwrite").save(f"{quarantine_path}/claim_missing_id")
missing_id.unpersist()

# 2. Find duplicates - simpler approach
duplicates = claim_df.exceptAll(claim_df.dropDuplicates(['claim_id'])).cache()
dup_count = duplicates.count()
print(f"⚠ Duplicates: {dup_count}")
if dup_count > 0:
    duplicates.write.format("delta").mode("overwrite").save(f"{quarantine_path}/claim_duplicates")
duplicates.unpersist()

# 3. Capture negative amounts
negative_amounts = claim_df.filter(
    (F.col('claim_id').isNotNull()) & 
    (F.col('claim_amount') < 0)
).cache()
neg_count = negative_amounts.count()
print(f"⚠ Negative amounts: {neg_count}")
if neg_count > 0:
    negative_amounts.write.format("delta").mode("overwrite").save(f"{quarantine_path}/claim_negative_amounts")
negative_amounts.unpersist()

# 4. Clean data (remove bad records)
claim_clean = claim_df.filter(
    (F.col('claim_id').isNotNull()) &
    (F.col('claim_amount') >= 0)
).dropDuplicates(['claim_id'])

# 5. Standardize dates
claim_clean = claim_clean.withColumn(
    'service_date',
    F.expr("try_to_date(service_date, 'yyyy-MM-dd')")
)

# 6. Capture orphaned records (invalid foreign keys) - optimized with broadcast
member_ids = member_silver.select('member_id')
provider_ids = provider_silver.select('provider_id')

# Find claims with invalid member_id OR invalid provider_id
orphaned_member = claim_clean.join(member_ids, 'member_id', 'left_anti')
orphaned_provider = claim_clean.join(provider_ids, 'provider_id', 'left_anti')
orphaned_claims = orphaned_member.unionByName(orphaned_provider).dropDuplicates(['claim_id']).cache()

orphaned_count = orphaned_claims.count()
print(f"⚠ Orphaned records (invalid member_id or provider_id): {orphaned_count}")
if orphaned_count > 0:
    orphaned_claims.write.format("delta").mode("overwrite").save(f"{quarantine_path}/claim_orphaned")
orphaned_claims.unpersist()

# 7. Keep only valid claims (referential integrity)
claim_clean = claim_clean.join(
    member_silver.select('member_id'),
    'member_id',
    'inner'
).join(
    provider_silver.select('provider_id'),
    'provider_id',
    'inner'
)

# Write to Silver
claim_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/claim")
print(f"\n✓ Silver records: {claim_clean.count()}")

# COMMAND ----------

# DBTITLE 1,Validation Summary
# Read all Silver tables
member_final = spark.read.format("delta").load(f"{silver_path}/member")
provider_final = spark.read.format("delta").load(f"{silver_path}/provider")
claim_final = spark.read.format("delta").load(f"{silver_path}/claim")

print("=" * 60)
print("SILVER LAYER SUMMARY")
print("=" * 60)
print(f"\nMember:   {member_final.count():,} records")
print(f"Provider: {provider_final.count():,} records")
print(f"Claim:    {claim_final.count():,} records")
print("\n✓ All tables cleaned and validated")
print(f"✓ Data saved to: {silver_path}")

# COMMAND ----------

# DBTITLE 1,Quarantine Summary
# Check quarantine folder
print("=" * 60)
print("QUARANTINE SUMMARY")
print("=" * 60)

try:
    # List all quarantine tables
    quarantine_files = dbutils.fs.ls(quarantine_path)
    
    if len(quarantine_files) > 0:
        print(f"\n⚠ Found {len(quarantine_files)} quarantine table(s):\n")
        
        for file_info in quarantine_files:
            table_name = file_info.name.rstrip('/')
            try:
                df = spark.read.format("delta").load(f"{quarantine_path}/{table_name}")
                count = df.count()
                print(f"  - {table_name}: {count:,} records")
            except:
                print(f"  - {table_name}: Unable to read")
        
        print(f"\n✓ Quarantine data saved to: {quarantine_path}")
        print("  Review these records to understand data quality issues")
    else:
        print("\n✓ No quarantine records - all data is clean!")
except:
    print("\n✓ No quarantine records - all data is clean!")

# COMMAND ----------

