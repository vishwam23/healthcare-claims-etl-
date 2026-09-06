# Healthcare Claims ETL Pipeline

End-to-end data pipeline for processing healthcare claims data using Databricks Medallion Architecture (Bronze → Silver → Gold).

## Project Structure

```
healthcare-claims-etl/
├── notebooks/
│   ├── bronze.py          # Raw data ingestion (CSV → Delta)
│   ├── silver.py          # Data cleaning, validation & quarantine
│   └── gold.py            # Business aggregations & analytics
├── data/
│   ├── raw/               # Source CSV files
│   ├── bronze/            # Delta tables (raw)
│   ├── silver/            # Delta tables (clean)
│   ├── quarantine/        # Invalid/suspicious records
│   ├── gold/              # Aggregated analytics tables
│   └── README.md          # Data folder documentation
└── README.md
```

## Medallion Architecture

### 🥉 Bronze Layer (Raw Ingestion)
- **Source**: Raw CSV files from `/Volumes/healthcare/raw_data/raw`
- **Target**: Delta tables in `/Volumes/healthcare/bronze_data/bronze`
- **Purpose**: Initial data ingestion with **no transformations**
- **Tables**: `claim` (82 records), `member` (50 members), `provider` (70 providers)
- **Format**: Parquet + Delta Log

### 🥈 Silver Layer (Cleaned & Validated)
- **Source**: Bronze Delta tables
- **Target**: Delta tables in `/Volumes/healthcare/silver_data/silver`
- **Purpose**: Data cleaning, validation, and quality checks
- **Features**:
  - ✅ Remove duplicate records
  - ✅ Validate primary keys (claim_id, member_id, provider_id)
  - ✅ Quarantine invalid data (missing IDs, negative amounts)
  - ✅ Standardize date formats (yyyy-MM-dd)
  - ✅ Enforce referential integrity (orphaned claims)
  - ✅ Apply business rules
- **Data Quality Score**: ~85-95%

### 🥇 Gold Layer (Business Aggregations)
- **Source**: Silver Delta tables
- **Target**: Delta tables in `/Volumes/healthcare/gold_data/gold`
- **Purpose**: Business-ready aggregated tables for analytics and reporting
- **Tables**:
  - `provider_summary` - Claims by provider with totals
  - `member_summary` - Claims by member with history
  - `monthly_trends` - Time-series analysis
  - `diagnosis_summary` - Top diagnosis codes

## Data Quality Framework

### Validation Checks

| Check Type | Rule | Action |
|------------|------|--------|
| **Primary Keys** | claim_id, member_id, provider_id NOT NULL | ❌ Quarantine |
| **Duplicates** | Duplicate IDs | ❌ Quarantine |
| **Referential Integrity** | claim.member_id exists in member | ❌ Quarantine orphaned |
| **Business Rules** | claim_amount >= 0 | ❌ Quarantine negative amounts |
| **Date Format** | All dates in yyyy-MM-dd | ⚠ Standardize |

### Data Quality Pipeline

```
Bronze (All Data)
    ↓
  Validate
    ↓
  ├─── Valid → Silver (Clean Data)
  └─── Invalid → Quarantine (Review)
```

## 🚨 Quarantine System

Invalid records are isolated for review in `/Volumes/healthcare/silver_data/quarantine`:

| Quarantine Table | Issue | Typical Count |
|------------------|-------|---------------|
| `claim_missing_id` | Missing claim_id | 0 |
| `claim_negative_amounts` | Negative amounts | 3 |
| `claim_orphaned` | Invalid foreign keys | 33 |
| `member_missing_id` | Missing member_id | 0 |
| `member_duplicates` | Duplicate members | 0 |
| `provider_missing_id` | Missing provider_id | 0 |
| `provider_duplicates` | Duplicate providers | 0 |

## 🚀 Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Serverless compute or cluster with DBR 13.3+
- Sample CSV files in `/Volumes/healthcare/raw_data/raw`

### Run the Pipeline

**Step 1: Bronze Layer** - Load raw CSV files
```bash
# Open: notebooks/bronze.py
# Run all cells
# Output: Delta tables in bronze volume
```

**Step 2: Silver Layer** - Clean and validate
```bash
# Open: notebooks/silver.py
# Run all cells
# Output: Clean tables in silver + quarantine tables
```

**Step 3: Gold Layer** - Create aggregations
```bash
# Open: notebooks/gold.py
# Run all cells
# Output: Analytics tables in gold volume
```

### Expected Results

```
Bronze:  82 claims, 50 members, 70 providers
Silver:  ~49 claims, 50 members, 70 providers (after quality checks)
Gold:    4 aggregated analytics tables
```

## 📊 Pipeline Metrics

| Metric | Value |
|--------|-------|
| **Total Source Records** | 202 (82 claims + 50 members + 70 providers) |
| **Bronze Ingestion Rate** | 100% (all records accepted) |
| **Silver Quality Rate** | 81.19% (38 records quarantined) |
| **Gold Tables Created** | 4 aggregation tables |
| **Data Quality Score** | 81.19% (FAIR - Review quarantine) |
| **Processing Time** | ~30 seconds (Bronze → Silver → Gold) |

## 🛠️ Technologies

| Component | Technology |
|-----------|------------|
| **Platform** | Databricks (AWS) |
| **Storage Format** | Delta Lake |
| **Compute** | Serverless (CPU) |
| **Languages** | Python, SQL |
| **Architecture** | Medallion (Bronze-Silver-Gold) |
| **Catalog** | Unity Catalog |
| **Version Control** | Git (GitHub) |

## 📝 Sample Data Schema

### Claim Table
```
claim_id (PK), member_id (FK), provider_id (FK), 
diagnosis_code, claim_amount, service_date, claim_status
```

### Member Table
```
member_id (PK), member_name, dob, gender, state, plan_type
```

### Provider Table
```
provider_id (PK), provider_name, specialty, state
```

## 👥 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 💫 Future Enhancements

- [ ] Add incremental loading (CDC)
- [ ] Implement data quality monitoring
- [ ] Add alerting for failed quality checks
- [ ] Create dashboards for business metrics
- [ ] Add automated testing
- [ ] Implement data lineage tracking

## 📞 Contact

**Author**: Vishwam  
**Project**: Healthcare Claims ETL Pipeline  
**Repository**: [github.com/vishwam23/healthcare-claims-etl-](https://github.com/vishwam23/healthcare-claims-etl-)

## 📜 License

MIT License - Feel free to use this project for learning and development!
