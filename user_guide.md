# User Guide  
**Real-Time Data Ingestion with Spark Structured Streaming & PostgreSQL**

This guide provides step-by-step instructions to run, reset, and verify the
real-time streaming pipeline implemented in this Lab.

---

## Project Structure

```text
Lab 2/
├── user_guide.md 
├── config/
│   ├── .env
│   └── postgres_setup.sql
├── reports/
│   ├── performance_metrics.md
│   ├── project_overview.md
│   ├── system_architecture.png
│   └── test_cases.md
├── src/
│   ├── data_generator.py
│   └── spark_streaming_to_postgres.py        
└── .gitignore
```
## Documentation
- [`Project Overview`](reports/project_overview.md): A full reflection and architecture contextual document.
- [`System Architecture Diagram`](reports/system_architecture.png): A visual representation of the end-to-end data flow.
- [`Test Cases`](reports/test_cases.md):  A detailed manual test plan documentating validation steps, expected outcomes, and observed results.
- [`Performance Metrics`](reports/performance_metrics.md): An analysis of system performance. 

---

## Prerequisites
- Apache Spark available via `spark-submit`
- PostgreSQL 
- Python (tested in a Conda environment)
- PostgreSQL JDBC driver resolved via Spark package manager
- Linux / WSL2 environment recommended

---

## Initial Setup

### Database Setup
Create the required schema and table by running:
```bash
psql -h localhost -U <username> -d <database> -f config/postgres_setup.sql
```
## Resetting State

This is recommended before each run for clear and reproducible results, and to
avoid interference from previously ingested data or persisted streaming state.

### Reset PostgreSQL Table
Clearing all previously ingested records from the target table for latency mfeasurements to depend/reflect on current execution:
```sql
TRUNCATE TABLE rt.ecommerce_events;
```
### Reset Streaming State
This prevents Spark from resuming from an earlier checkpoint and guarantees
that all incoming files are treated as new events.

First ensure these directories exist in your repo.
```bash
rm -rf data/incoming/*
rm -rf "data/checkpoint"
mkdir -p "data/checkpoint"
```
## Running the Pipeline
### Starting the Data Generator
Run this to generate the CSV files incrementally in the input directory:
```bash
python src/data_generator.py --run_minutes 1
```
**NB:** The minutes can change

### Starting the Spark Streaming Job
To process newly arrived files in micro-batches, apply
transformations and validation logic, and write the results to PostgreSQL, run:
```bash
python src/data_generator.py --run_minutes 1
```
**NB:** The seconds can change

To verify the results, you can check the number of ingested records in postgresql.

