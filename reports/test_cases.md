# Test Cases Report


## Pre-Test Reset Procedure
Before running the tests, both storage and streaming state are reset to ensure
repeatability.

### PostgreSQL Reset
Reseting the table (Removing/deleting its prior elements)
```sql
TRUNCATE TABLE rt.ecommerce_events;

```
### Spark State Reset
```bash
rm -rf data/incoming/*
rm -rf "data/checkpoint"
mkdir -p "data/checkpoint"
```
### Expected Result

1. PostgreSQL table is empty
2. No existing input backlog
3. No previous streaming checkpoint state

### Observed Result
Reset completed successfully

## Test Cases 

### Summary

| Test Case ID | Description                              | Status |
|-------------|------------------------------------------|--------|
| TC-01       | CSV file generation                      | PASS   |
| TC-02       | Spark detects incoming files             | PASS   |
| TC-03       | Micro-batch processing behavior          | PASS   |
| TC-04       | Data persistence in PostgreSQL           | PASS   |
| TC-05       | Transformation and data quality          | PASS   |
| TC-06       | Graceful streaming termination           | PASS   |
| TC-07       | Performance sanity check                 | PASS   |

### TC-01: CSV File Generation

**Objective**  
Verify that the event generator produces valid CSV files in the landing directory.

**Steps**
```bash
python src/data_generator.py --run_minutes 1
head -n 2 data/incoming/events_*.csv
```
**Expected**

- CSV files appear in data/incoming/
- Files contain headers and event records

**Observed**

- CSV files generated successfully with valid structure

### TC-02: Spark Detects Incoming Files

**Objective**  
Confirm that Spark Structured Streaming detects new CSV files as they arrive.

**Steps**
```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.3 \
  src/spark_streaming_to_postgres.py --run_seconds 60
```
**Expected**

- Spark initializes without errors
- Micro-batches are triggered automatically

**Observed**

- Spark detected new files and started batch processing
### TC-03: Micro-Batch Processing Behavior

**Objective**  
Validate batch size variability and processing time stability.

**Observed Spark Logs Example**
```text
[batch 0] rows=600 ... batch_time=3.73s
[batch 1] rows=200 ... batch_time=0.95s
[batch 2] rows=200 ... batch_time=0.74s
[batch 3] rows=200 ... batch_time=0.71s
[batch 4] rows=200 ... batch_time=0.33s
```
**Expected**

- Batch sizes vary depending on file arrival
- Batch processing time remains below the trigger interval

**Observed**

- Stable micro-batch execution with consistent processing times

### TC-04: Data Persistence in PostgreSQL

**Objective**  
Verify that processed records are written to the database.

**Steps**
```sql
SELECT COUNT(*) FROM rt.ecommerce_events;
```
**Expected**

- Row count increases as streaming progresses

**Observed**

- Final row count after timed execution: 1600 records. This matches the batch rows in the spark logs.
### TC-05: Transformation and Data Quality Validation

**Objective**  
Ensure transformation and cleaning rules are applied correctly.
**Checks**
```sql
SELECT event_type, COUNT(*) 
FROM rt.ecommerce_events
GROUP BY event_type;

SELECT COUNT(*) 
FROM rt.ecommerce_events
WHERE event_type = 'view' AND total_amount <> 0;

```
**Expected**

- Only `view` and purchase event `types`
- `view` events have `total_amount` = 0

**Observed**

- Transformation logic applied correctly.

### TC-06: Graceful Streaming Termination

**Objective**  

Validate that the streaming job stops cleanly after the configured duration.
**Steps**
Allow Spark to exit automatically after `--run_seconds 60`

**Expected**

- No fatal errors
- No partial or corrupted writes

**Observed**

- Streaming job terminated cleanly

### TC-07: Performance Sanity Check
**Objective**  
Verify that the system processes data within reasonable performance bounds
relative to the configured trigger interval.

**Checks**
- Micro-batch processing time is less than the trigger interval
- Streaming throughput is non-zero and stable
- No backlog accumulation is observed during steady-state execution

**Observed Spark Logs**
- batch_time = 0.33s – 3.73s
- trigger interval = 10s
- throughput ≈ 35–60 rows/s


**Expected**
- Batch processing completes before the next trigger
- Throughput remains consistent across batches

**Observed**
- All batches completed well within the trigger interval
- No performance degradation observed

**Conclusion**
- Performance is within expected limits for the test workload
### Important Test Questions

The following table maps the validation questions to the executed test cases:

| Question                                         | Covered By |
|------------------------------------------------------------------|------------|
| Are the CSV files being generated correctly?                      | TC-01      |
| Is Spark detecting and processing new files?                     | TC-02, TC-03 |
| Are the data transformations correct?                            | TC-05      |
| Is data being written into PostgreSQL without errors?            | TC-04, TC-06 |
| Are performance metrics within expected limits?                  | TC-03, TC-07 |

