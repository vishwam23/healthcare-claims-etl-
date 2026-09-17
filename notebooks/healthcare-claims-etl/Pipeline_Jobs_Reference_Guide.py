# Databricks notebook source
# DBTITLE 1,Databricks Orchestration - Quick Reference Guide
# MAGIC %md
# MAGIC # Databricks Orchestration - Quick Reference Guide
# MAGIC
# MAGIC **Purpose:** Comprehensive notes on Jobs, Pipelines, and ETL orchestration in Databricks
# MAGIC
# MAGIC **Use Case:** Healthcare Claims ETL (Bronze → Silver → Gold)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Table of Contents
# MAGIC 1. Overview: Jobs vs Pipelines
# MAGIC 2. Option A: Ingestion Pipeline
# MAGIC 3. Option B: ETL Pipeline (SDP)
# MAGIC 4. Option C: Jobs (Lakeflow Jobs)
# MAGIC 5. Decision Matrix
# MAGIC 6. Common Functions & Syntax
# MAGIC 7. Quick Reference Glossary

# COMMAND ----------

# DBTITLE 1,1. Overview: Jobs vs Pipelines
# MAGIC %md
# MAGIC ## 1. Overview: Jobs vs Pipelines
# MAGIC
# MAGIC ### Three Main Options in Databricks
# MAGIC
# MAGIC | Option | Type | Code Required | Best For |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **Ingestion Pipeline** | Managed Connector | Minimal/No-code | Landing raw data from external sources |
# MAGIC | **ETL Pipeline (SDP)** | Declarative Framework | Declarative Python/SQL | Multi-layer transformations with quality checks |
# MAGIC | **Job (Lakeflow Jobs)** | Task Orchestrator | Use existing code | Scheduling existing notebooks/scripts |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Differences
# MAGIC
# MAGIC **Imperative (Notebooks/Jobs):**
# MAGIC - You write: "Read this, then do that, then save here"
# MAGIC - Manual control over execution order
# MAGIC - Explicit error handling
# MAGIC
# MAGIC **Declarative (SDP):**
# MAGIC - You write: "This table should look like this"
# MAGIC - System figures out execution order automatically
# MAGIC - Built-in error handling and retries

# COMMAND ----------

# DBTITLE 1,2. Ingestion Pipeline (Managed Ingestion)
# MAGIC %md
# MAGIC ## 2. Ingestion Pipeline (Managed Ingestion)
# MAGIC
# MAGIC ### What It Is
# MAGIC Pre-built, managed connectors to ingest data from external sources into Delta tables with minimal/no code.
# MAGIC
# MAGIC ### Key Features
# MAGIC - ✅ No-code or low-code configuration
# MAGIC - ✅ Automatic schema detection and evolution
# MAGIC - ✅ Continuous streaming or batch ingestion
# MAGIC - ✅ Built-in monitoring and error handling
# MAGIC - ✅ Supports cloud storage (S3, ADLS, GCS), Kafka, Kinesis, Pub/Sub
# MAGIC
# MAGIC ### When to Use
# MAGIC - Ingesting raw data from external sources
# MAGIC - Need automatic file monitoring (new files auto-ingested)
# MAGIC - Want schema evolution without code changes
# MAGIC - Just landing data (Bronze layer)
# MAGIC
# MAGIC ### Example Use Case
# MAGIC ```
# MAGIC S3 Bucket (new CSV files) → Ingestion Pipeline → Bronze Delta Table
# MAGIC ```
# MAGIC
# MAGIC ### Configuration (No Code)
# MAGIC - Select source (S3, ADLS, Kafka, etc.)
# MAGIC - Provide credentials
# MAGIC - Choose target schema/table
# MAGIC - Set refresh schedule
# MAGIC
# MAGIC **Output:** Bronze Delta tables (raw data, no transformations)

# COMMAND ----------

# DBTITLE 1,3. ETL Pipeline - Spark Declarative Pipeline (SDP)
# MAGIC %md
# MAGIC ## 3. ETL Pipeline - Spark Declarative Pipeline (SDP)
# MAGIC
# MAGIC ### What It Is
# MAGIC A managed transformation framework where you **declare** tables and their transformations. The system handles:
# MAGIC - Dependency resolution (automatic execution order)
# MAGIC - Incremental processing (only new/changed data)
# MAGIC - Data quality checks (expectations)
# MAGIC - Monitoring and lineage
# MAGIC - Error recovery and retries
# MAGIC
# MAGIC ### Key Features
# MAGIC - ✅ Declarative syntax (`@dlt.table` decorators)
# MAGIC - ✅ Automatic dependency graph
# MAGIC - ✅ Built-in data quality expectations
# MAGIC - ✅ Incremental processing out-of-the-box
# MAGIC - ✅ Visual lineage and monitoring
# MAGIC - ✅ Streaming + Batch support
# MAGIC - ✅ Schema evolution
# MAGIC
# MAGIC ### When to Use
# MAGIC - Building medallion architecture (Bronze → Silver → Gold)
# MAGIC - Need data quality validations (expectations)
# MAGIC - Want automatic dependency management
# MAGIC - Need streaming transformations
# MAGIC - Want rich monitoring and lineage tracking
# MAGIC
# MAGIC ### Core Concepts
# MAGIC
# MAGIC **Streaming Tables:**
# MAGIC - Always append-only
# MAGIC - Process data incrementally
# MAGIC - Used for: Raw ingestion, event streams
# MAGIC
# MAGIC **Materialized Views:**
# MAGIC - Can be updated/overwritten
# MAGIC - Automatically refresh when source changes
# MAGIC - Used for: Aggregations, dimensional tables
# MAGIC
# MAGIC **Views:**
# MAGIC - Not materialized, query-time computation
# MAGIC - No storage cost
# MAGIC - Used for: Lightweight transformations
# MAGIC
# MAGIC **Expectations (Data Quality):**
# MAGIC - `@dlt.expect()`: Log violations, continue
# MAGIC - `@dlt.expect_or_drop()`: Drop bad records
# MAGIC - `@dlt.expect_or_fail()`: Stop pipeline on violation

# COMMAND ----------

# DBTITLE 1,SDP Syntax Examples
# MAGIC %md
# MAGIC ### SDP Syntax Examples
# MAGIC
# MAGIC **Bronze Layer (Streaming Table):**
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Raw claims data from CSV"
# MAGIC )
# MAGIC def bronze_claims():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "csv")
# MAGIC         .option("header", "true")
# MAGIC         .load("/mnt/raw/claims/")
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Silver Layer (Materialized View with Quality Check):**
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     comment="Cleaned claims with valid amounts"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_amount", "claim_amount > 0")
# MAGIC @dlt.expect_or_drop("valid_date", "claim_date IS NOT NULL")
# MAGIC def silver_claims():
# MAGIC     return (
# MAGIC         dlt.read("bronze_claims")
# MAGIC         .filter(col("status") == "approved")
# MAGIC         .dropDuplicates(["claim_id"])
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Gold Layer (Aggregation):**
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     comment="Monthly claim aggregations by provider"
# MAGIC )
# MAGIC def gold_claims_summary():
# MAGIC     return (
# MAGIC         dlt.read("silver_claims")
# MAGIC         .groupBy("provider_id", month("claim_date").alias("month"))
# MAGIC         .agg(
# MAGIC             count("*").alias("total_claims"),
# MAGIC             sum("claim_amount").alias("total_amount")
# MAGIC         )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Key Differences from Regular Code:**
# MAGIC - Use `dlt.read()` instead of `spark.read.table()`
# MAGIC - Use `@dlt.table` decorator instead of `.write.saveAsTable()`
# MAGIC - No explicit save/write statements
# MAGIC - Dependencies detected automatically

# COMMAND ----------

# DBTITLE 1,4. Jobs (Lakeflow Jobs)
# MAGIC %md
# MAGIC ## 4. Jobs (Lakeflow Jobs)
# MAGIC
# MAGIC ### What It Is
# MAGIC Task orchestration system that schedules and runs existing workloads (notebooks, Python files, SQL, JARs, etc.) in a defined sequence.
# MAGIC
# MAGIC ### Key Features
# MAGIC - ✅ Use existing code as-is (no rewrite)
# MAGIC - ✅ Explicit task dependencies
# MAGIC - ✅ Mixed workload types (notebook + Python + SQL + API calls)
# MAGIC - ✅ Parameter passing between tasks
# MAGIC - ✅ Conditional logic and branching
# MAGIC - ✅ Email/webhook notifications
# MAGIC - ✅ Retry policies per task
# MAGIC
# MAGIC ### When to Use
# MAGIC - **Orchestrating existing notebooks** (your current situation!)
# MAGIC - Complex workflows with multiple types of tasks
# MAGIC - Need conditional logic ("if this fails, do that")
# MAGIC - Want to reuse existing code without rewriting
# MAGIC - Mix notebook + external API calls + notifications
# MAGIC
# MAGIC ### Job Structure
# MAGIC ```
# MAGIC Job: Healthcare Claims ETL
# MAGIC ├─ Task 1: Bronze Ingestion (notebook)
# MAGIC │   └─ Depends on: None
# MAGIC ├─ Task 2: Silver Transformation (notebook)
# MAGIC │   └─ Depends on: Task 1
# MAGIC └─ Task 3: Gold Aggregation (notebook)
# MAGIC     └─ Depends on: Task 2
# MAGIC ```
# MAGIC
# MAGIC ### Task Configuration
# MAGIC - **Task Name:** Descriptive name
# MAGIC - **Type:** Notebook, Python, SQL, JAR, dbt, etc.
# MAGIC - **Source:** Path to notebook/file
# MAGIC - **Cluster:** New, existing, or serverless
# MAGIC - **Parameters:** Key-value pairs passed to task
# MAGIC - **Dependencies:** Which tasks must succeed first
# MAGIC - **Retry Policy:** How many times to retry on failure
# MAGIC - **Timeout:** Max duration before killing task
# MAGIC
# MAGIC **Output:** Whatever your tasks produce (determined by task code)

# COMMAND ----------

# DBTITLE 1,5. Decision Matrix - Which to Choose?
# MAGIC %md
# MAGIC ## 5. Decision Matrix - Which to Choose?
# MAGIC
# MAGIC ### Quick Decision Tree
# MAGIC
# MAGIC **Question 1:** Do you already have working notebooks?
# MAGIC - **YES** → Use **Job** (no rewrite needed)
# MAGIC - **NO** → Continue to Question 2
# MAGIC
# MAGIC **Question 2:** Are you just landing raw data from external sources?
# MAGIC - **YES** → Use **Ingestion Pipeline**
# MAGIC - **NO** → Continue to Question 3
# MAGIC
# MAGIC **Question 3:** Do you need data quality checks and automatic incremental processing?
# MAGIC - **YES** → Use **SDP (ETL Pipeline)**
# MAGIC - **NO** → Use **Job** (simpler)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Detailed Comparison
# MAGIC
# MAGIC | Your Need | Recommended | Why |
# MAGIC | --- | --- | --- |
# MAGIC | Schedule 3 existing notebooks | **Job** | No code changes, quick setup |
# MAGIC | Ingest S3/ADLS files with no code | **Ingestion Pipeline** | Managed, auto-schema |
# MAGIC | Data quality expectations required | **SDP** | Built-in `@dlt.expect()` |
# MAGIC | Automatic incremental processing | **SDP** | Handles incrementals automatically |
# MAGIC | Streaming transformations | **SDP** or **Ingestion** | Real-time processing |
# MAGIC | Mix notebooks + API + emails | **Job** | Supports any task type |
# MAGIC | Need visual lineage graphs | **SDP** | Built-in lineage |
# MAGIC | Complex branching logic | **Job** | Conditional task execution |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Production Patterns
# MAGIC
# MAGIC **Pattern 1: Job Orchestration (Simplest)**
# MAGIC ```
# MAGIC Job → Bronze Notebook → Silver Notebook → Gold Notebook
# MAGIC ```
# MAGIC
# MAGIC **Pattern 2: Ingestion + SDP**
# MAGIC ```
# MAGIC Ingestion Pipeline (S3 → Bronze) → SDP Pipeline (Bronze → Silver → Gold)
# MAGIC ```
# MAGIC
# MAGIC **Pattern 3: Ingestion + Job**
# MAGIC ```
# MAGIC Ingestion Pipeline (Kafka → Bronze) → Job (Silver + Gold notebooks)
# MAGIC ```
# MAGIC
# MAGIC **Pattern 4: Full SDP (Most Managed)**
# MAGIC ```
# MAGIC SDP Pipeline (Auto Loader → Bronze → Silver → Gold, all in one)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,6. Common Functions & Syntax Reference
# MAGIC %md
# MAGIC ## 6. Common Functions & Syntax Reference
# MAGIC
# MAGIC ### SDP (Spark Declarative Pipeline) Functions
# MAGIC
# MAGIC #### Core Decorators
# MAGIC ```python
# MAGIC @dlt.table()                          # Define a materialized table
# MAGIC @dlt.view()                           # Define a non-materialized view
# MAGIC @dlt.table(name="custom_name")        # Custom table name
# MAGIC @dlt.table(comment="Description")     # Add table documentation
# MAGIC ```
# MAGIC
# MAGIC #### Data Quality (Expectations)
# MAGIC ```python
# MAGIC @dlt.expect("rule_name", "condition")              # Log violations, continue
# MAGIC @dlt.expect_or_drop("rule_name", "condition")     # Drop violating rows
# MAGIC @dlt.expect_or_fail("rule_name", "condition")     # Stop pipeline on violation
# MAGIC @dlt.expect_all({"rule1": "cond1", "rule2": "cond2"})  # Multiple expectations
# MAGIC ```
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("valid_amount", "claim_amount > 0")
# MAGIC @dlt.expect_or_drop("valid_date", "claim_date IS NOT NULL")
# MAGIC ```
# MAGIC
# MAGIC #### Reading Data
# MAGIC ```python
# MAGIC dlt.read("table_name")                # Read from another DLT table
# MAGIC dlt.read_stream("table_name")         # Streaming read from DLT table
# MAGIC spark.read.table("catalog.schema.table")  # Read from external table
# MAGIC ```
# MAGIC
# MAGIC #### Auto Loader (Streaming Ingestion)
# MAGIC ```python
# MAGIC spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")      # File format
# MAGIC     .option("cloudFiles.schemaLocation", path)  # Schema checkpoint
# MAGIC     .option("header", "true")
# MAGIC     .load("/path/to/files/")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Job (Workflow) Functions
# MAGIC
# MAGIC #### Task Configuration (via UI or API)
# MAGIC ```python
# MAGIC # Databricks SDK example
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC job = w.jobs.create(
# MAGIC     name="Healthcare Claims ETL",
# MAGIC     tasks=[
# MAGIC         {
# MAGIC             "task_key": "bronze",
# MAGIC             "notebook_task": {"notebook_path": "/path/to/bronze"},
# MAGIC             "new_cluster": {...}
# MAGIC         },
# MAGIC         {
# MAGIC             "task_key": "silver",
# MAGIC             "depends_on": [{"task_key": "bronze"}],
# MAGIC             "notebook_task": {"notebook_path": "/path/to/silver"},
# MAGIC         }
# MAGIC     ],
# MAGIC     schedule={"quartz_cron_expression": "0 0 2 * * ?"}  # Daily at 2 AM
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Passing Parameters Between Tasks
# MAGIC ```python
# MAGIC # In source task (e.g., bronze notebook)
# MAGIC dbutils.jobs.taskValues.set(key="row_count", value=1000)
# MAGIC
# MAGIC # In downstream task (e.g., silver notebook)
# MAGIC row_count = dbutils.jobs.taskValues.get(
# MAGIC     taskKey="bronze",
# MAGIC     key="row_count"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Getting Job Context
# MAGIC ```python
# MAGIC dbutils.notebook.entry_point.getDbutils()
# MAGIC     .notebook().getContext().jobId().get()       # Job ID
# MAGIC     
# MAGIC dbutils.notebook.entry_point.getDbutils()
# MAGIC     .notebook().getContext().currentRunId().get() # Run ID
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Delta Lake Functions (Used in All Approaches)
# MAGIC
# MAGIC ```python
# MAGIC # Read Delta
# MAGIC df = spark.read.format("delta").load("/path/to/table")
# MAGIC df = spark.read.table("catalog.schema.table")
# MAGIC
# MAGIC # Write Delta
# MAGIC df.write.format("delta").mode("overwrite").save("/path")
# MAGIC df.write.mode("append").saveAsTable("catalog.schema.table")
# MAGIC
# MAGIC # Optimize
# MAGIC spark.sql("OPTIMIZE catalog.schema.table")
# MAGIC spark.sql("VACUUM catalog.schema.table RETAIN 168 HOURS")
# MAGIC
# MAGIC # Time Travel
# MAGIC df = spark.read.format("delta").option("versionAsOf", 1).load("/path")
# MAGIC df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("/path")
# MAGIC
# MAGIC # Merge (Upsert)
# MAGIC from delta.tables import DeltaTable
# MAGIC target = DeltaTable.forPath(spark, "/path/to/target")
# MAGIC target.alias("t").merge(
# MAGIC     source.alias("s"),
# MAGIC     "t.id = s.id"
# MAGIC ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,7. Quick Reference Glossary
# MAGIC %md
# MAGIC ## 7. Quick Reference Glossary
# MAGIC
# MAGIC ### Key Terms (One-Liners)
# MAGIC
# MAGIC **Auto Loader:** Streaming file ingestion that automatically detects new files in cloud storage
# MAGIC
# MAGIC **Bronze Layer:** Raw data ingestion zone with minimal transformations
# MAGIC
# MAGIC **Silver Layer:** Cleaned, validated, deduplicated data ready for analysis
# MAGIC
# MAGIC **Gold Layer:** Business-level aggregations and metrics
# MAGIC
# MAGIC **Medallion Architecture:** Bronze → Silver → Gold layered data design pattern
# MAGIC
# MAGIC **Delta Lake:** Open-source storage layer providing ACID transactions on data lakes
# MAGIC
# MAGIC **DLT:** Old name for Spark Declarative Pipelines (SDP)
# MAGIC
# MAGIC **SDP (Spark Declarative Pipeline):** Declarative framework for building multi-layer ETL pipelines
# MAGIC
# MAGIC **Lakeflow Jobs:** Databricks workflow orchestration (formerly called "Workflows")
# MAGIC
# MAGIC **Streaming Table:** SDP table that processes data incrementally (append-only)
# MAGIC
# MAGIC **Materialized View:** SDP table that can be updated/overwritten
# MAGIC
# MAGIC **Expectation:** Data quality rule in SDP (validate, drop, or fail on violation)
# MAGIC
# MAGIC **Task:** Single unit of work in a Job (notebook, Python, SQL, etc.)
# MAGIC
# MAGIC **Task Dependency:** Defines execution order (Task B runs after Task A succeeds)
# MAGIC
# MAGIC **Serverless Compute:** Auto-scaling compute with no cluster management
# MAGIC
# MAGIC **Unity Catalog:** Unified governance layer for data and AI assets
# MAGIC
# MAGIC **Volume:** Unity Catalog object for storing non-tabular files
# MAGIC
# MAGIC **Schema:** Container for tables/views/volumes (like a database)
# MAGIC
# MAGIC **Catalog:** Top-level container for schemas (multi-tenant isolation)
# MAGIC
# MAGIC **Time Travel:** Query historical versions of Delta tables
# MAGIC
# MAGIC **Optimize:** Compact small Delta files for better read performance
# MAGIC
# MAGIC **Vacuum:** Delete old Delta file versions to save storage
# MAGIC
# MAGIC **Z-Ordering:** Co-locate related data in Delta files for faster queries
# MAGIC
# MAGIC **Liquid Clustering:** Automatic incremental clustering (replaces Z-Ordering)
# MAGIC
# MAGIC **Merge/Upsert:** Insert new records and update existing ones in a single operation
# MAGIC
# MAGIC **Incremental Processing:** Only process new/changed data, not full recompute
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Function Quick Reference
# MAGIC
# MAGIC | Function | Purpose | Example |
# MAGIC | --- | --- | --- |
# MAGIC | `@dlt.table()` | Define a table in SDP | `@dlt.table()` |
# MAGIC | `@dlt.expect_or_drop()` | Drop bad records | `@dlt.expect_or_drop("valid", "col > 0")` |
# MAGIC | `dlt.read()` | Read DLT table | `dlt.read("bronze_claims")` |
# MAGIC | `spark.read.format("delta")` | Read Delta table | `spark.read.format("delta").load("/path")` |
# MAGIC | `.write.format("delta")` | Write Delta table | `df.write.format("delta").save("/path")` |
# MAGIC | `DeltaTable.forPath().merge()` | Upsert operation | `target.merge(source, "id").execute()` |
# MAGIC | `display()` | Show DataFrame/results | `display(df)` |
# MAGIC | `spark.readStream` | Streaming read | `spark.readStream.format("cloudFiles")` |
# MAGIC | `dbutils.jobs.taskValues.set()` | Pass value to next task | `dbutils.jobs.taskValues.set("key", value)` |
# MAGIC | `dbutils.jobs.taskValues.get()` | Get value from prev task | `dbutils.jobs.taskValues.get("task", "key")` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Scheduling (Cron Examples)
# MAGIC
# MAGIC ```
# MAGIC # Every day at 2 AM
# MAGIC 0 0 2 * * ?
# MAGIC
# MAGIC # Every hour
# MAGIC 0 0 * * * ?
# MAGIC
# MAGIC # Every 15 minutes
# MAGIC 0 */15 * * * ?
# MAGIC
# MAGIC # Weekdays at 9 AM
# MAGIC 0 0 9 ? * MON-FRI
# MAGIC
# MAGIC # First day of month at midnight
# MAGIC 0 0 0 1 * ?
# MAGIC ```
# MAGIC
# MAGIC Format: `second minute hour day month day-of-week year`

# COMMAND ----------

# DBTITLE 1,8. Revision Checklist
# MAGIC %md
# MAGIC ## 8. Revision Checklist
# MAGIC
# MAGIC ### Before Interview/Review
# MAGIC
# MAGIC **Conceptual Understanding:**
# MAGIC - [ ] Can explain imperative vs declarative approach
# MAGIC - [ ] Know when to use Job vs SDP vs Ingestion Pipeline
# MAGIC - [ ] Understand medallion architecture (Bronze/Silver/Gold)
# MAGIC - [ ] Know difference between streaming table and materialized view
# MAGIC - [ ] Understand data quality expectations (log, drop, fail)
# MAGIC
# MAGIC **Technical Skills:**
# MAGIC - [ ] Write `@dlt.table()` definition with expectations
# MAGIC - [ ] Configure Auto Loader for file ingestion
# MAGIC - [ ] Set up Job with task dependencies
# MAGIC - [ ] Pass parameters between job tasks
# MAGIC - [ ] Write Delta merge/upsert operation
# MAGIC - [ ] Read from Delta with time travel
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - [ ] Know when to use `.cache()` (and when not to)
# MAGIC - [ ] Understand incremental vs full refresh
# MAGIC - [ ] Know when to `OPTIMIZE` and `VACUUM`
# MAGIC - [ ] Understand Unity Catalog three-level namespace (catalog.schema.table)
# MAGIC
# MAGIC **Your Healthcare Project:**
# MAGIC - [ ] Explain the 3 notebooks (bronze, silver, gold)
# MAGIC - [ ] Know the data sources (claim, member, provider CSVs)
# MAGIC - [ ] Describe transformations at each layer
# MAGIC - [ ] Explain quality checks in silver layer
# MAGIC - [ ] Know aggregations in gold layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Quick Self-Test Questions
# MAGIC
# MAGIC 1. **When would you choose a Job over SDP?**
# MAGIC    - *Answer: When you have existing notebooks and want quick orchestration without rewriting code*
# MAGIC
# MAGIC 2. **What's the difference between `@dlt.expect()` and `@dlt.expect_or_drop()`?**
# MAGIC    - *Answer: expect() logs violations and continues; expect_or_drop() removes bad rows*
# MAGIC
# MAGIC 3. **How does SDP know which table to process first?**
# MAGIC    - *Answer: Automatic dependency detection based on `dlt.read()` references*
# MAGIC
# MAGIC 4. **When would you use an Ingestion Pipeline?**
# MAGIC    - *Answer: Landing raw files from cloud storage/Kafka with no-code auto-schema detection*
# MAGIC
# MAGIC 5. **What's the purpose of the Bronze layer?**
# MAGIC    - *Answer: Store raw, unmodified data exactly as received from source*

# COMMAND ----------

# DBTITLE 1,9. Healthcare Claims Project - Your Implementation
# MAGIC %md
# MAGIC ## 9. Healthcare Claims Project - Your Implementation
# MAGIC
# MAGIC ### Current Setup (3 Notebooks)
# MAGIC
# MAGIC **Notebook 1: Bronze Layer**
# MAGIC - **Path:** `/Users/vishwam23042002@gmail.com/healthcare-claims-etl/notebooks/bronze`
# MAGIC - **Purpose:** Load raw CSV files into Delta tables
# MAGIC - **Source:** `/Volumes/healthcare/raw_data/raw/*.csv`
# MAGIC - **Target:** `/Volumes/healthcare/bronze_data/bronze/` (Delta format)
# MAGIC - **Tables:** `claim`, `member`, `provider`
# MAGIC - **Transformations:** None (raw data only)
# MAGIC - **Key Functions:** `spark.read.csv()`, `.write.format("delta").save()`
# MAGIC
# MAGIC **Notebook 2: Silver Layer**
# MAGIC - **Purpose:** Clean and validate data
# MAGIC - **Source:** Bronze Delta tables
# MAGIC - **Target:** Silver Delta tables
# MAGIC - **Transformations:**
# MAGIC   - Data type casting
# MAGIC   - Null handling
# MAGIC   - Deduplication
# MAGIC   - Data quality checks (quarantine bad records)
# MAGIC   - Column standardization
# MAGIC
# MAGIC **Notebook 3: Gold Layer**
# MAGIC - **Purpose:** Create business-ready aggregations
# MAGIC - **Source:** Silver Delta tables
# MAGIC - **Target:** Gold Delta tables (analytical/reporting tables)
# MAGIC - **Transformations:**
# MAGIC   - Aggregations by provider, date, etc.
# MAGIC   - KPIs and metrics
# MAGIC   - Dimensional modeling
# MAGIC   - Optimized for BI/reporting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Recommended Next Step: Create a Job
# MAGIC
# MAGIC **Why Job (not SDP)?**
# MAGIC - Your notebooks are complete and tested
# MAGIC - No rewrite needed
# MAGIC - Quick to implement
# MAGIC - Easy to maintain
# MAGIC
# MAGIC **Job Structure:**
# MAGIC ```
# MAGIC Job Name: Healthcare Claims Daily ETL
# MAGIC
# MAGIC ├─ Task 1: "Bronze Ingestion"
# MAGIC │   ├─ Type: Notebook
# MAGIC │   ├─ Source: /Users/.../bronze
# MAGIC │   ├─ Compute: Serverless
# MAGIC │   └─ Depends on: None
# MAGIC │
# MAGIC ├─ Task 2: "Silver Transformation"
# MAGIC │   ├─ Type: Notebook
# MAGIC │   ├─ Source: /Users/.../silver
# MAGIC │   ├─ Compute: Serverless
# MAGIC │   └─ Depends on: Task 1
# MAGIC │
# MAGIC └─ Task 3: "Gold Aggregation"
# MAGIC     ├─ Type: Notebook
# MAGIC     ├─ Source: /Users/.../gold
# MAGIC     ├─ Compute: Serverless
# MAGIC     └─ Depends on: Task 2
# MAGIC
# MAGIC Schedule: Daily at 2:00 AM UTC
# MAGIC Alerts: Email on failure
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,10. Common Pitfalls & Troubleshooting
# MAGIC %md
# MAGIC ## 10. Common Pitfalls & Troubleshooting
# MAGIC
# MAGIC ### Jobs
# MAGIC
# MAGIC **Problem:** Task fails but no clear error
# MAGIC - **Solution:** Check task logs (click task → "View Logs")
# MAGIC
# MAGIC **Problem:** Tasks run in wrong order
# MAGIC - **Solution:** Verify task dependencies are set correctly
# MAGIC
# MAGIC **Problem:** Downstream task uses old data
# MAGIC - **Solution:** Ensure upstream task writes data before downstream reads
# MAGIC
# MAGIC **Problem:** Job runs too long
# MAGIC - **Solution:** 
# MAGIC   - Enable serverless compute (faster start)
# MAGIC   - Optimize Delta tables (`OPTIMIZE`, `VACUUM`)
# MAGIC   - Use `.cache()` for reused DataFrames
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### SDP (Spark Declarative Pipelines)
# MAGIC
# MAGIC **Problem:** "Table X not found"
# MAGIC - **Solution:** Check spelling in `dlt.read("table_name")`, ensure table is defined
# MAGIC
# MAGIC **Problem:** Circular dependency error
# MAGIC - **Solution:** Table A reads from Table B, which reads from Table A (break the cycle)
# MAGIC
# MAGIC **Problem:** Expectations failing
# MAGIC - **Solution:** Review expectation rules, check if too strict, consider `expect()` instead of `expect_or_drop()`
# MAGIC
# MAGIC **Problem:** Pipeline runs forever
# MAGIC - **Solution:** Streaming table without `.limit()` in development, use Update mode for batch testing
# MAGIC
# MAGIC **Problem:** Schema evolution breaks pipeline
# MAGIC - **Solution:** Use `.option("mergeSchema", "true")` or set `spark.databricks.delta.schema.autoMerge.enabled = true`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Delta Lake
# MAGIC
# MAGIC **Problem:** "ConcurrentAppendException"
# MAGIC - **Solution:** Multiple writers to same table without proper isolation, use Delta merge or enable optimistic concurrency
# MAGIC
# MAGIC **Problem:** Slow queries on large tables
# MAGIC - **Solution:** 
# MAGIC   - Run `OPTIMIZE table_name`
# MAGIC   - Add Z-Ordering: `OPTIMIZE table_name ZORDER BY (column)`
# MAGIC   - Enable Liquid Clustering (newer alternative)
# MAGIC
# MAGIC **Problem:** Storage costs growing
# MAGIC - **Solution:** Run `VACUUM table_name RETAIN 168 HOURS` (default 7 days)
# MAGIC
# MAGIC **Problem:** "Table schema mismatch"
# MAGIC - **Solution:** Enable schema evolution or manually align schemas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### General Best Practices
# MAGIC
# MAGIC **DO:**
# MAGIC - ✅ Use serverless compute for auto-scaling
# MAGIC - ✅ Partition large tables by date for faster queries
# MAGIC - ✅ Use Delta format for all data (ACID transactions)
# MAGIC - ✅ Set up alerts for job failures
# MAGIC - ✅ Document expectations and transformations
# MAGIC - ✅ Test with small data samples first
# MAGIC - ✅ Use Unity Catalog for governance
# MAGIC
# MAGIC **DON'T:**
# MAGIC - ❌ Use `.cache()` everywhere (only for reused DataFrames)
# MAGIC - ❌ Skip `OPTIMIZE` on production tables
# MAGIC - ❌ Mix Delta and Parquet formats
# MAGIC - ❌ Forget to handle late-arriving data
# MAGIC - ❌ Use `overwrite` mode in production without backup
# MAGIC - ❌ Ignore data quality checks
# MAGIC - ❌ Run `VACUUM` with < 7 days retention (breaks time travel)

# COMMAND ----------

# DBTITLE 1,11. Summary: Jobs vs SDP - Final Comparison
# MAGIC %md
# MAGIC ## 11. Summary: Jobs vs SDP - Final Comparison
# MAGIC
# MAGIC ### When You Have Existing Notebooks (Your Situation)
# MAGIC
# MAGIC | Factor | Job | SDP |
# MAGIC | --- | --- | --- |
# MAGIC | **Setup Time** | 15 minutes | 2-4 hours (rewrite) |
# MAGIC | **Code Changes** | None | Complete rewrite |
# MAGIC | **Complexity** | Low | Medium-High |
# MAGIC | **Flexibility** | High (any code) | Medium (declarative only) |
# MAGIC | **Monitoring** | Basic | Rich (lineage, expectations) |
# MAGIC | **Incremental Processing** | Manual | Automatic |
# MAGIC | **Data Quality** | Manual checks | Built-in expectations |
# MAGIC | **Learning Curve** | Minimal | Moderate |
# MAGIC | **Best For** | **Quick orchestration** | Long-term managed pipelines |
# MAGIC
# MAGIC ### **Recommendation for Healthcare Claims Project:**
# MAGIC
# MAGIC **Phase 1 (Now):** Use **Job**
# MAGIC - Quick to implement
# MAGIC - Learn orchestration basics
# MAGIC - Get pipeline running in production
# MAGIC
# MAGIC **Phase 2 (Later, if needed):** Migrate to **SDP**
# MAGIC - When you need automatic incremental processing
# MAGIC - When data quality becomes critical
# MAGIC - When pipeline grows more complex
# MAGIC - When you need detailed lineage tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Migration Path (Job → SDP)
# MAGIC
# MAGIC If you later decide to migrate:
# MAGIC
# MAGIC **Step 1:** Start with Bronze
# MAGIC ```python
# MAGIC # Current: Bronze notebook
# MAGIC df = spark.read.csv("/path/to/raw/")
# MAGIC df.write.format("delta").save("/path/to/bronze/")
# MAGIC
# MAGIC # Migrate to: SDP
# MAGIC @dlt.table()
# MAGIC def bronze_claims():
# MAGIC     return spark.readStream.format("cloudFiles").load("/path/to/raw/")
# MAGIC ```
# MAGIC
# MAGIC **Step 2:** Add Silver with expectations
# MAGIC ```python
# MAGIC @dlt.table()
# MAGIC @dlt.expect_or_drop("valid_amount", "amount > 0")
# MAGIC def silver_claims():
# MAGIC     return dlt.read("bronze_claims").filter(...)
# MAGIC ```
# MAGIC
# MAGIC **Step 3:** Add Gold aggregations
# MAGIC ```python
# MAGIC @dlt.table()
# MAGIC def gold_claims_summary():
# MAGIC     return dlt.read("silver_claims").groupBy(...).agg(...)
# MAGIC ```
# MAGIC
# MAGIC **Step 4:** Deploy as single SDP pipeline
# MAGIC - All three layers in one pipeline
# MAGIC - Automatic dependency management
# MAGIC - Single monitoring dashboard
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Final Decision Framework
# MAGIC
# MAGIC **Choose Job if:**
# MAGIC - You want to go live quickly
# MAGIC - You have working code already
# MAGIC - You're still learning Databricks
# MAGIC - You need maximum flexibility
# MAGIC - Your workflow mixes different tools (notebook + API + email)
# MAGIC
# MAGIC **Choose SDP if:**
# MAGIC - Starting from scratch
# MAGIC - Data quality is critical
# MAGIC - You want automatic incrementals
# MAGIC - You need streaming transformations
# MAGIC - You want rich monitoring and lineage
# MAGIC - You're building for long-term production
# MAGIC
# MAGIC **Choose Ingestion Pipeline if:**
# MAGIC - Just landing raw files
# MAGIC - Want zero-code solution
# MAGIC - Need automatic schema evolution
# MAGIC - Monitoring source for new files
# MAGIC
# MAGIC **Your Healthcare Claims → Job is the right choice for now!**

# COMMAND ----------

