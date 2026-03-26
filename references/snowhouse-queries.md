# SNOWHOUSE Query Templates — Query Optimization Finder

## Data Source Reference

| View | Location | Purpose |
|------|----------|---------|
| ACCOUNT_ETL_V | `SNOWHOUSE_IMPORT.PROD` | Account locator → ID + deployment |
| JOB_ETL_V | `SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>` | Query history + stats |
| TABLE_ETL_V | `SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>` | Table metadata (cluster keys) |
| WAREHOUSE_ETL_V | `SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>` | Warehouse metadata (use SIZE not SIZE_ID) |

## JOB_ETL_V Available Columns
UUID, JOB_ID, PARENT_JOB_ID, DESCRIPTION, TOTAL_DURATION (ms), STATS (variant), ERROR_CODE, CREATED_ON, WAREHOUSE_NAME, QUERY_PARAMETERIZED_HASH, ACCOUNT_ID

## JOB_ETL_V Does NOT Have
EXECUTION_STATUS, STATUS, WAREHOUSE_SIZE, BYTES_SCANNED

## STATS Variant Paths
```sql
j.STATS:stats:scanBytes::NUMBER
j.STATS:stats:numRowsInserted::NUMBER
j.STATS:stats:numRowsUpdated::NUMBER
j.STATS:stats:numRowsDeleted::NUMBER
j.STATS:stats:ioLocalTempWriteBytes::NUMBER
j.STATS:stats:ioRemoteTempWriteBytes::NUMBER
j.STATS:stats:hashJoinNumberBroadcastDecisions::NUMBER
j.STATS:stats:outputBytes::NUMBER              -- ALWAYS 0/NULL in SNOWHOUSE — do not use for ratios
j.STATS:stats:numPartitionsScanned::NUMBER      -- Often NULL — use TABLE_ETL_V fallback
j.STATS:stats:numPartitionsTotal::NUMBER        -- Often NULL — use TABLE_ETL_V fallback
```

---

## Step 1: Account Resolution

**Direct lookup (by locator):**
```sql
USE WAREHOUSE SE_WH;
SELECT ID AS ACCOUNT_ID, NAME AS LOCATOR, DEPLOYMENT, COMPANY_NAME
FROM SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V
WHERE NAME = '<LOCATOR>';
```

**If no results, use Raven Account Lookup** to resolve org-level aliases (e.g., `ENT_PR`) to locators. Then confirm in SNOWHOUSE with the resolved locator.

**IMPORTANT:** `PARENT_JOB_ID` for top-level queries can be `NULL` or `0` depending on deployment. Always filter with `(PARENT_JOB_ID IS NULL OR PARENT_JOB_ID = 0)`.

---

## Step 2: Candidate Harvest Query

This is the primary discovery query. It aggregates all query patterns over 30 days and surfaces the top 50 by total runtime.

```sql
WITH raw AS (
    SELECT
        j.QUERY_PARAMETERIZED_HASH,
        j.UUID,
        j.JOB_ID,
        j.PARENT_JOB_ID,
        j.DESCRIPTION,
        j.TOTAL_DURATION,
        j.WAREHOUSE_NAME,
        j.ERROR_CODE,
        j.CREATED_ON,
        j.STATS,
        CASE
            WHEN j.DESCRIPTION ILIKE 'CALL %' THEN 'CALL'
            WHEN j.DESCRIPTION ILIKE 'MERGE %' THEN 'MERGE'
            WHEN j.DESCRIPTION ILIKE 'INSERT %' THEN 'INSERT'
            WHEN j.DESCRIPTION ILIKE 'UPDATE %' THEN 'UPDATE'
            WHEN j.DESCRIPTION ILIKE 'DELETE %' THEN 'DELETE'
            WHEN j.DESCRIPTION ILIKE 'SELECT %' THEN 'SELECT'
            WHEN j.DESCRIPTION ILIKE 'COPY %' THEN 'COPY'
            WHEN j.DESCRIPTION ILIKE 'CREATE %' THEN 'CREATE'
            ELSE 'OTHER'
        END AS QUERY_TYPE
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V j
    WHERE j.ACCOUNT_ID = <ACCOUNT_ID>
      AND j.CREATED_ON >= DATEADD(day, -30, CURRENT_TIMESTAMP())
      AND (j.PARENT_JOB_ID IS NULL OR j.PARENT_JOB_ID = 0)
      AND j.DESCRIPTION NOT ILIKE 'SHOW %'
      AND j.DESCRIPTION NOT ILIKE 'DESCRIBE %'
      AND j.DESCRIPTION NOT ILIKE 'DESC %'
      AND j.DESCRIPTION NOT ILIKE 'EXPLAIN %'
      AND j.DESCRIPTION NOT ILIKE 'USE %'
      AND j.DESCRIPTION NOT ILIKE 'ALTER SESSION %'
      AND j.DESCRIPTION NOT ILIKE 'GET %'
      AND j.DESCRIPTION NOT ILIKE 'PUT %'
      AND j.DESCRIPTION NOT ILIKE 'LIST %'
      AND j.DESCRIPTION NOT ILIKE 'REMOVE %'
      AND j.DESCRIPTION NOT ILIKE 'SET %'
      AND j.DESCRIPTION NOT ILIKE 'UNSET %'
      AND j.DESCRIPTION NOT ILIKE 'GRANT %'
      AND j.DESCRIPTION NOT ILIKE 'REVOKE %'
      AND j.WAREHOUSE_NAME NOT IN ('COMPUTE_SERVICE_WH', 'SYSTEM$CLUSTERING_WH')
      AND j.DESCRIPTION NOT ILIKE '%trust_center%'
      AND j.DESCRIPTION NOT ILIKE '%SYSTEM$AUTO_REFRESH%'
      AND j.DESCRIPTION NOT ILIKE 'ALTER DATABASE%ENABLE REPLICATION%'
      AND j.DESCRIPTION NOT ILIKE 'ALTER DATABASE%ENABLE FAILOVER%'
      AND j.DESCRIPTION NOT ILIKE '%SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER%'
),
aggregated AS (
    SELECT
        r.QUERY_PARAMETERIZED_HASH,
        COUNT(*) AS TOTAL_RUNS,
        ROUND(SUM(r.TOTAL_DURATION) / 60000.0, 1) AS TOTAL_DURATION_MIN,
        ROUND(AVG(r.TOTAL_DURATION) / 60000.0, 2) AS AVG_DURATION_MIN,
        ROUND(MEDIAN(r.TOTAL_DURATION) / 60000.0, 2) AS MEDIAN_DURATION_MIN,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY r.TOTAL_DURATION) / 60000.0, 2) AS P95_DURATION_MIN,
        ROUND(MAX(r.TOTAL_DURATION) / 60000.0, 2) AS MAX_DURATION_MIN,
        ROUND(SUM(NVL(r.STATS:stats:scanBytes::NUMBER, 0)) / POWER(1024,4), 3) AS TOTAL_SCAN_TB,
        ROUND(SUM(NVL(r.STATS:stats:ioLocalTempWriteBytes::NUMBER, 0) + NVL(r.STATS:stats:ioRemoteTempWriteBytes::NUMBER, 0)) / POWER(1024,3), 1) AS TOTAL_SPILL_GB,
        ROUND(SUM(NVL(r.STATS:stats:ioRemoteTempWriteBytes::NUMBER, 0)) / POWER(1024,3), 1) AS TOTAL_REMOTE_SPILL_GB,
        SUM(CASE WHEN r.ERROR_CODE IS NOT NULL THEN 1 ELSE 0 END) AS ERROR_COUNT,
        MAX_BY(r.UUID, r.TOTAL_DURATION) AS SAMPLE_UUID_SLOWEST,
        MAX_BY(r.UUID, r.CREATED_ON) AS SAMPLE_UUID_RECENT,
        MAX_BY(SUBSTRING(r.DESCRIPTION, 1, 300), r.CREATED_ON) AS SAMPLE_DESCRIPTION,
        MAX_BY(r.QUERY_TYPE, r.CREATED_ON) AS QUERY_TYPE,
        MAX_BY(r.WAREHOUSE_NAME, r.CREATED_ON) AS WAREHOUSE_NAME,
        MIN(r.CREATED_ON) AS FIRST_SEEN,
        MAX(r.CREATED_ON) AS LAST_SEEN
    FROM raw r
    GROUP BY r.QUERY_PARAMETERIZED_HASH
)
SELECT *
FROM aggregated
WHERE (TOTAL_RUNS >= 2 OR MAX_DURATION_MIN >= 30)
  AND AVG_DURATION_MIN >= 0.167
ORDER BY TOTAL_DURATION_MIN DESC
LIMIT 50;
```

**Notes:**
- `PARENT_JOB_ID IS NULL` ensures we only look at top-level queries (not children of SPs)
- `AVG_DURATION_MIN >= 0.167` = 10 seconds minimum average
- `MAX_BY(UUID, TOTAL_DURATION)` picks the slowest execution as `SAMPLE_UUID_SLOWEST` for enrichment (worst-case signal detection)
- `MAX_BY(UUID, CREATED_ON)` picks the most recent execution as `SAMPLE_UUID_RECENT` for deep-dive commands and trend freshness
- Single-execution queries are included only if they exceeded 30 minutes
- `QUERY_TYPE` derived from DESCRIPTION prefix for quick filtering

---

## Step 3a: SP Child Pattern Analysis

Run for each CALL-type candidate using its `SAMPLE_UUID_SLOWEST`.

```sql
WITH parent AS (
    SELECT JOB_ID, TOTAL_DURATION
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V
    WHERE ACCOUNT_ID = <ACCOUNT_ID>
      AND UUID = '<SAMPLE_UUID_SLOWEST>'
),
children AS (
    SELECT
        c.QUERY_PARAMETERIZED_HASH AS CHILD_HASH,
        c.DESCRIPTION AS CHILD_DESC,
        c.TOTAL_DURATION AS CHILD_DURATION,
        c.JOB_ID AS CHILD_JOB_ID,
        c.STATS,
        CASE WHEN c.DESCRIPTION ILIKE 'CALL %' THEN TRUE ELSE FALSE END AS IS_NESTED_CALL,
        NVL(c.STATS:stats:scanBytes::NUMBER, 0) AS SCAN_BYTES,
        NVL(c.STATS:stats:numRowsInserted::NUMBER, 0) AS ROWS_INSERTED,
        NVL(c.STATS:stats:numRowsUpdated::NUMBER, 0) AS ROWS_UPDATED,
        NVL(c.STATS:stats:numRowsDeleted::NUMBER, 0) AS ROWS_DELETED,
        NVL(c.STATS:stats:ioLocalTempWriteBytes::NUMBER, 0) + NVL(c.STATS:stats:ioRemoteTempWriteBytes::NUMBER, 0) AS SPILL_BYTES,
        NVL(c.STATS:stats:hashJoinNumberBroadcastDecisions::NUMBER, 0) AS BROADCAST_JOINS,
        NVL(c.STATS:stats:outputBytes::NUMBER, 0) AS OUTPUT_BYTES
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V c
    JOIN parent p ON c.PARENT_JOB_ID = p.JOB_ID
    WHERE c.ACCOUNT_ID = <ACCOUNT_ID>
)
SELECT
    COUNT(DISTINCT CHILD_HASH) AS DISTINCT_CHILD_HASHES,
    COUNT(*) AS TOTAL_CHILDREN,
    MAX(CASE WHEN CNT > 5 THEN TRUE ELSE FALSE END) AS HAS_LOOP_SIGNAL,
    MAX(CHILD_DURATION) AS MAX_CHILD_DURATION_MS,
    ROUND(MAX(CHILD_DURATION) * 100.0 / NULLIF((SELECT TOTAL_DURATION FROM parent), 0), 1) AS BOTTLENECK_CONCENTRATION_PCT,
    MAX(IS_NESTED_CALL) AS HAS_NESTED_CALLS,
    SUM(BROADCAST_JOINS) AS TOTAL_BROADCAST_JOINS,
    ROUND(SUM(SCAN_BYTES) / POWER(1024,3), 1) AS TOTAL_CHILD_SCAN_GB,
    ROUND(SUM(SPILL_BYTES) / POWER(1024,3), 1) AS TOTAL_CHILD_SPILL_GB,
    ROUND(SUM(OUTPUT_BYTES) / POWER(1024,3), 1) AS TOTAL_CHILD_OUTPUT_GB
FROM (
    SELECT
        CHILD_HASH,
        CHILD_DESC,
        CHILD_DURATION,
        IS_NESTED_CALL,
        SCAN_BYTES,
        ROWS_INSERTED,
        ROWS_UPDATED,
        ROWS_DELETED,
        SPILL_BYTES,
        BROADCAST_JOINS,
        OUTPUT_BYTES,
        COUNT(*) OVER (PARTITION BY CHILD_HASH) AS CNT
    FROM children
);
```

**Loop detection shortcut:** If `HAS_LOOP_SIGNAL = TRUE` and `TOTAL_CHILDREN > 100`, this is almost certainly Pattern 1 (Cursor Loop) — score accordingly.

---

## Step 3b: Child Hash Breakdown (for loop detection detail)

Run only when `HAS_LOOP_SIGNAL = TRUE` from Step 3a.

```sql
WITH parent AS (
    SELECT JOB_ID
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V
    WHERE ACCOUNT_ID = <ACCOUNT_ID>
      AND UUID = '<SAMPLE_UUID_SLOWEST>'
)
SELECT
    c.QUERY_PARAMETERIZED_HASH,
    SUBSTRING(c.DESCRIPTION, 1, 150) AS SAMPLE_DESC,
    COUNT(*) AS ITERATIONS,
    ROUND(SUM(c.TOTAL_DURATION) / 1000.0, 1) AS TOTAL_SEC,
    ROUND(SUM(NVL(c.STATS:stats:scanBytes::NUMBER, 0)) / POWER(1024,3), 1) AS TOTAL_SCAN_GB,
    SUM(NVL(c.STATS:stats:numRowsInserted::NUMBER, 0)) AS TOTAL_ROWS_INSERTED,
    ROUND(AVG(NVL(c.STATS:stats:numRowsInserted::NUMBER, 0)), 0) AS AVG_ROWS_PER_ITERATION
FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V c
JOIN parent p ON c.PARENT_JOB_ID = p.JOB_ID
WHERE c.ACCOUNT_ID = <ACCOUNT_ID>
GROUP BY c.QUERY_PARAMETERIZED_HASH, SAMPLE_DESC
ORDER BY TOTAL_SEC DESC;
```

---

## Step 3c: Scan Efficiency & Partition Pruning

Run for each DML-type candidate (MERGE, INSERT, UPDATE, DELETE). Use `SAMPLE_UUID_SLOWEST` for worst-case signal detection.

```sql
SELECT
    j.UUID,
    ROUND(NVL(j.STATS:stats:scanBytes::NUMBER, 0) / POWER(1024,3), 2) AS SCAN_GB,
    ROUND(NVL(j.STATS:stats:scanBytes::NUMBER, 0) / POWER(1024,3) / NULLIF(j.TOTAL_DURATION / 60000.0, 0), 2) AS SCAN_INTENSITY_GBPM,
    j.STATS:stats:numPartitionsScanned::NUMBER AS PARTITIONS_SCANNED,
    j.STATS:stats:numPartitionsTotal::NUMBER AS PARTITIONS_TOTAL,
    ROUND(DIV0(j.STATS:stats:numPartitionsScanned::NUMBER, j.STATS:stats:numPartitionsTotal::NUMBER) * 100, 1) AS PRUNING_RATIO_PCT,
    NVL(j.STATS:stats:hashJoinNumberBroadcastDecisions::NUMBER, 0) AS BROADCAST_JOINS,
    NVL(j.STATS:stats:numRowsInserted::NUMBER, 0) AS ROWS_INSERTED,
    NVL(j.STATS:stats:numRowsUpdated::NUMBER, 0) AS ROWS_UPDATED,
    NVL(j.STATS:stats:numRowsDeleted::NUMBER, 0) AS ROWS_DELETED,
    ROUND(NVL(j.STATS:stats:ioLocalTempWriteBytes::NUMBER, 0) / POWER(1024,3), 2) AS LOCAL_SPILL_GB,
    ROUND(NVL(j.STATS:stats:ioRemoteTempWriteBytes::NUMBER, 0) / POWER(1024,3), 2) AS REMOTE_SPILL_GB
FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V j
WHERE j.ACCOUNT_ID = <ACCOUNT_ID>
  AND j.UUID = '<SAMPLE_UUID_SLOWEST>';
```

**Key signals:**
- `SCAN_INTENSITY_GBPM > 100` → scanning >100 GB/min for a query that takes >5 min suggests full-table scans or missing pruning
- `PRUNING_RATIO_PCT > 90` → no meaningful partition pruning (clustering candidate). **When NULL** (common in SNOWHOUSE), fall back to: MERGE/DELETE type + SCAN_GB > 50 + no cluster key from Step 3f = strong unclustered signal
- `BROADCAST_JOINS > 5` → missing temp table statistics
- `ROWS_INSERTED >> ROWS_UPDATED` on MERGE → possible fan-out or mismatched join

**IMPORTANT:** `outputBytes` is always 0/NULL in SNOWHOUSE. Do NOT use `SCAN_TO_OUTPUT_RATIO`. Use `SCAN_INTENSITY_GBPM` (scan GB per minute of runtime) as a proxy for scan efficiency.

---

## Step 3d: Trend Analysis

Run for each top-50 candidate to classify the performance trend.

```sql
WITH weekly AS (
    SELECT
        CASE
            WHEN j.CREATED_ON >= DATEADD(day, -7, CURRENT_TIMESTAMP()) THEN 'LATEST_WEEK'
            WHEN j.CREATED_ON >= DATEADD(day, -14, CURRENT_TIMESTAMP()) THEN 'PRIOR_WEEK'
            ELSE 'OLDER'
        END AS PERIOD,
        j.TOTAL_DURATION
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V j
    WHERE j.ACCOUNT_ID = <ACCOUNT_ID>
      AND j.QUERY_PARAMETERIZED_HASH = '<HASH>'
      AND j.CREATED_ON >= DATEADD(day, -30, CURRENT_TIMESTAMP())
)
SELECT
    COUNT(CASE WHEN PERIOD = 'LATEST_WEEK' THEN 1 END) AS LATEST_WEEK_RUNS,
    ROUND(AVG(CASE WHEN PERIOD = 'LATEST_WEEK' THEN TOTAL_DURATION END) / 60000.0, 2) AS LATEST_WEEK_AVG_MIN,
    COUNT(CASE WHEN PERIOD = 'PRIOR_WEEK' THEN 1 END) AS PRIOR_WEEK_RUNS,
    ROUND(AVG(CASE WHEN PERIOD = 'PRIOR_WEEK' THEN TOTAL_DURATION END) / 60000.0, 2) AS PRIOR_WEEK_AVG_MIN,
    ROUND(MEDIAN(TOTAL_DURATION) / 60000.0, 2) AS OVERALL_MEDIAN_MIN,
    ROUND(MAX(TOTAL_DURATION) / 60000.0, 2) AS OVERALL_MAX_MIN
FROM weekly;
```

**Trend classification logic:**
- `DEGRADING`: `LATEST_WEEK_AVG_MIN > PRIOR_WEEK_AVG_MIN * 1.2` (>20% increase)
- `STABLE-SLOW`: Variance <15% across weeks AND `AVG_DURATION_MIN > 5`
- `INTERMITTENT`: `OVERALL_MAX_MIN > 3 * OVERALL_MEDIAN_MIN`
- `HEALTHY`: None of the above (stable and within norms)

---

## Step 3f: Table Metadata Lookup

Run for MERGE/DELETE/UPDATE candidates to check clustering status. Parse the primary table name from DESCRIPTION and look up its cluster key.

```sql
SELECT
    t.TABLE_NAME,
    t.TABLE_SCHEMA,
    t.TABLE_CATALOG,
    t.CLUSTER_BY_KEYS
FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.TABLE_ETL_V t
WHERE t.ACCOUNT_ID = <ACCOUNT_ID>
  AND t.TABLE_NAME IN ('<TABLE_1>', '<TABLE_2>', ...)
ORDER BY t.TABLE_NAME;
```

**How to parse table names from DESCRIPTION:**
- MERGE: table name follows `MERGE INTO` (e.g., `MERGE INTO DB.SCHEMA.TABLE` → `TABLE`)
- DELETE: table name follows `DELETE FROM` (e.g., `DELETE FROM DB.SCHEMA.TABLE` → `TABLE`)
- UPDATE: table name follows `UPDATE` (e.g., `UPDATE DB.SCHEMA.TABLE` → `TABLE`)
- Strip database/schema prefix, use just the table name for TABLE_ETL_V lookup

**Key signals:**
- `CLUSTER_BY_KEYS` is empty/NULL AND SCAN_GB > 50 for MERGE/DELETE → HIGH confidence unclustered target (Pattern 5/8)
- `CLUSTER_BY_KEYS` exists but SCAN_GB is still very high → cluster key may not match the query's filter predicate

---

## Step 3g: Warehouse Size Lookup

Run once for all distinct warehouse names across the top 50 candidates.

```sql
SELECT DISTINCT
    w.NAME AS WAREHOUSE_NAME,
    w.SIZE AS WAREHOUSE_SIZE
FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.WAREHOUSE_ETL_V w
WHERE w.ACCOUNT_ID = <ACCOUNT_ID>
  AND w.NAME IN ('<WH_1>', '<WH_2>', ...);
```

**Warehouse size → credits/hour mapping:**
| Size | Credits/Hour |
|------|--------------|
| X-Small | 1 |
| Small | 2 |
| Medium | 4 |
| Large | 8 |
| X-Large | 16 |
| 2X-Large | 32 |
| 3X-Large | 64 |
| 4X-Large | 128 |
| 5X-Large | 256 |
| 6X-Large | 512 |

**Key signals:**
- Remote spill on SIZE ≤ Large → strong size-up recommendation (easy win)
- Remote spill on SIZE ≥ 2X-Large → likely a data volume issue, not just sizing. Recommend query optimization over size-up.

---

## Step 3e: Serial Execution Detection

Run for CALL-type candidates to detect Pattern 6 (serial independent work) and Pattern 7 (repeated identical joins).

```sql
WITH parent AS (
    SELECT JOB_ID
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V
    WHERE ACCOUNT_ID = <ACCOUNT_ID>
      AND UUID = '<SAMPLE_UUID_SLOWEST>'
),
children AS (
    SELECT
        c.QUERY_PARAMETERIZED_HASH,
        SUBSTRING(c.DESCRIPTION, 1, 200) AS DESC_SNIPPET,
        c.TOTAL_DURATION,
        c.CREATED_ON,
        ROW_NUMBER() OVER (ORDER BY c.CREATED_ON) AS SEQ
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V c
    JOIN parent p ON c.PARENT_JOB_ID = p.JOB_ID
    WHERE c.ACCOUNT_ID = <ACCOUNT_ID>
)
SELECT
    QUERY_PARAMETERIZED_HASH,
    COUNT(*) AS REPETITIONS,
    MIN(DESC_SNIPPET) AS SAMPLE_DESC,
    ROUND(SUM(TOTAL_DURATION) / 1000.0, 1) AS TOTAL_SEC,
    ROUND(AVG(TOTAL_DURATION) / 1000.0, 1) AS AVG_SEC,
    MIN(SEQ) AS FIRST_SEQ,
    MAX(SEQ) AS LAST_SEQ
FROM children
GROUP BY QUERY_PARAMETERIZED_HASH
ORDER BY REPETITIONS DESC;
```

**Signal interpretation:**
- Top hash has `REPETITIONS > 5` and descriptions differ only by a parameter → Pattern 7
- Multiple hashes each with 1 repetition, similar durations, no dependencies → Pattern 6 candidate
- Single hash dominates runtime → bottleneck concentration (Pattern 1 if >100, or single heavy child)

---

## Batch Enrichment Strategy

To avoid running 50+ individual queries, batch the enrichment:

**Batch 1: All candidates scan/spill/pruning** (single query using SAMPLE_UUID_SLOWEST for all 50):
```sql
SELECT
    j.UUID,
    j.QUERY_PARAMETERIZED_HASH,
    ROUND(NVL(j.STATS:stats:scanBytes::NUMBER, 0) / POWER(1024,3), 2) AS SCAN_GB,
    ROUND(NVL(j.STATS:stats:scanBytes::NUMBER, 0) / POWER(1024,3) / NULLIF(j.TOTAL_DURATION / 60000.0, 0), 2) AS SCAN_INTENSITY_GBPM,
    j.STATS:stats:numPartitionsScanned::NUMBER AS PARTITIONS_SCANNED,
    j.STATS:stats:numPartitionsTotal::NUMBER AS PARTITIONS_TOTAL,
    ROUND(DIV0(j.STATS:stats:numPartitionsScanned::NUMBER, j.STATS:stats:numPartitionsTotal::NUMBER) * 100, 1) AS PRUNING_RATIO_PCT,
    NVL(j.STATS:stats:hashJoinNumberBroadcastDecisions::NUMBER, 0) AS BROADCAST_JOINS,
    NVL(j.STATS:stats:numRowsInserted::NUMBER, 0) AS ROWS_INSERTED,
    NVL(j.STATS:stats:numRowsUpdated::NUMBER, 0) AS ROWS_UPDATED,
    NVL(j.STATS:stats:numRowsDeleted::NUMBER, 0) AS ROWS_DELETED,
    ROUND(NVL(j.STATS:stats:ioLocalTempWriteBytes::NUMBER, 0) / POWER(1024,3), 2) AS LOCAL_SPILL_GB,
    ROUND(NVL(j.STATS:stats:ioRemoteTempWriteBytes::NUMBER, 0) / POWER(1024,3), 2) AS REMOTE_SPILL_GB
FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V j
WHERE j.ACCOUNT_ID = <ACCOUNT_ID>
  AND j.UUID IN ('<SLOWEST_UUID_1>', '<SLOWEST_UUID_2>', ... '<SLOWEST_UUID_50>');
```

**Batch 2: CALL candidates child summary** (single query for all CALL UUIDs):
```sql
WITH parents AS (
    SELECT UUID, JOB_ID
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V
    WHERE ACCOUNT_ID = <ACCOUNT_ID>
      AND UUID IN ('<CALL_UUID_1>', '<CALL_UUID_2>', ...)
),
children AS (
    SELECT
        p.UUID AS PARENT_UUID,
        c.QUERY_PARAMETERIZED_HASH AS CHILD_HASH,
        c.TOTAL_DURATION,
        c.DESCRIPTION,
        CASE WHEN c.DESCRIPTION ILIKE 'CALL %' THEN TRUE ELSE FALSE END AS IS_NESTED_CALL,
        NVL(c.STATS:stats:scanBytes::NUMBER, 0) AS SCAN_BYTES,
        NVL(c.STATS:stats:ioLocalTempWriteBytes::NUMBER, 0) + NVL(c.STATS:stats:ioRemoteTempWriteBytes::NUMBER, 0) AS SPILL_BYTES,
        NVL(c.STATS:stats:hashJoinNumberBroadcastDecisions::NUMBER, 0) AS BROADCAST_JOINS
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V c
    JOIN parents p ON c.PARENT_JOB_ID = p.JOB_ID
    WHERE c.ACCOUNT_ID = <ACCOUNT_ID>
)
SELECT
    PARENT_UUID,
    COUNT(DISTINCT CHILD_HASH) AS DISTINCT_HASHES,
    COUNT(*) AS TOTAL_CHILDREN,
    MAX(CASE WHEN CNT > 5 THEN TRUE ELSE FALSE END) AS HAS_LOOP_SIGNAL,
    MAX(CASE WHEN CNT > 100 THEN TRUE ELSE FALSE END) AS HAS_STRONG_LOOP_SIGNAL,
    MAX(IS_NESTED_CALL) AS HAS_NESTED_CALLS,
    SUM(BROADCAST_JOINS) AS TOTAL_BROADCAST_JOINS,
    ROUND(SUM(SCAN_BYTES) / POWER(1024,3), 1) AS TOTAL_CHILD_SCAN_GB,
    ROUND(SUM(SPILL_BYTES) / POWER(1024,3), 1) AS TOTAL_CHILD_SPILL_GB
FROM (
    SELECT *, COUNT(*) OVER (PARTITION BY PARENT_UUID, CHILD_HASH) AS CNT
    FROM children
)
GROUP BY PARENT_UUID;
```

**Batch 3: Warehouse size lookup** (single query for all distinct warehouses):
```sql
SELECT DISTINCT
    w.NAME AS WAREHOUSE_NAME,
    w.SIZE AS WAREHOUSE_SIZE
FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.WAREHOUSE_ETL_V w
WHERE w.ACCOUNT_ID = <ACCOUNT_ID>
  AND w.NAME IN ('<WH_1>', '<WH_2>', ...);
```

**Batch 4: Table metadata for MERGE/DELETE/UPDATE candidates** (cluster key check):
```sql
SELECT
    t.TABLE_NAME,
    t.TABLE_SCHEMA,
    t.TABLE_CATALOG,
    t.CLUSTER_BY_KEYS
FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.TABLE_ETL_V t
WHERE t.ACCOUNT_ID = <ACCOUNT_ID>
  AND t.TABLE_NAME IN ('<TABLE_1>', '<TABLE_2>', ...);
```

**Batch 5: Trend analysis** (single query for all 50 hashes — **MANDATORY**, do not skip):
```sql
WITH candidates AS (
    SELECT column1 AS HASH FROM VALUES
    ('<HASH_1>'), ('<HASH_2>'), ... ('<HASH_50>')
),
weekly AS (
    SELECT
        j.QUERY_PARAMETERIZED_HASH AS HASH,
        CASE
            WHEN j.CREATED_ON >= DATEADD(day, -7, CURRENT_TIMESTAMP()) THEN 'LATEST'
            WHEN j.CREATED_ON >= DATEADD(day, -14, CURRENT_TIMESTAMP()) THEN 'PRIOR'
            ELSE 'OLDER'
        END AS PERIOD,
        j.TOTAL_DURATION
    FROM SNOWHOUSE_IMPORT_SHARE_DB.<DEPLOYMENT>.JOB_ETL_V j
    JOIN candidates c ON j.QUERY_PARAMETERIZED_HASH = c.HASH
    WHERE j.ACCOUNT_ID = <ACCOUNT_ID>
      AND j.CREATED_ON >= DATEADD(day, -30, CURRENT_TIMESTAMP())
)
SELECT
    HASH,
    COUNT(CASE WHEN PERIOD = 'LATEST' THEN 1 END) AS LATEST_RUNS,
    ROUND(AVG(CASE WHEN PERIOD = 'LATEST' THEN TOTAL_DURATION END) / 60000.0, 2) AS LATEST_AVG_MIN,
    ROUND(AVG(CASE WHEN PERIOD = 'PRIOR' THEN TOTAL_DURATION END) / 60000.0, 2) AS PRIOR_AVG_MIN,
    ROUND(MEDIAN(TOTAL_DURATION) / 60000.0, 2) AS MEDIAN_MIN,
    ROUND(MAX(TOTAL_DURATION) / 60000.0, 2) AS MAX_MIN
FROM weekly
GROUP BY HASH;
```
