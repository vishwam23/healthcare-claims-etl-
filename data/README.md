# Data Folder Structure

This folder represents the medallion architecture layers used in Databricks Unity Catalog Volumes.

## Folder Organization

```
data/
├── raw/          # Raw CSV files (source data)
├── bronze/       # Delta tables (raw ingestion, no transformations)
├── silver/       # Delta tables (cleaned & validated)
├── quarantine/   # Delta tables (invalid/suspicious records)
└── gold/         # Delta tables (business aggregations)
```

## Unity Catalog Volume Paths

### Raw Layer
- **Path**: `/Volumes/healthcare/raw_data/raw`
- **Format**: CSV files
- **Files**: claim.csv, member.csv, provider.csv

### Bronze Layer
- **Path**: `/Volumes/healthcare/bronze_data/bronze`
- **Format**: Delta Lake tables
- **Purpose**: Raw data ingestion with no transformations

### Silver Layer
- **Path**: `/Volumes/healthcare/silver_data/silver`
- **Format**: Delta Lake tables
- **Purpose**: Cleaned, validated, and deduplicated data

### Quarantine Layer
- **Path**: `/Volumes/healthcare/silver_data/quarantine`
- **Format**: Delta Lake tables
- **Purpose**: Store invalid/suspicious records for review

### Gold Layer
- **Path**: `/Volumes/healthcare/gold_data/gold`
- **Format**: Delta Lake tables
- **Purpose**: Business-ready aggregated tables for analytics

See main README for full details.
