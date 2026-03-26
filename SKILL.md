---
name: query-optimization-finder
description: "Scan a customer account via SNOWHOUSE to discover and rank the top 10 query patterns with the highest optimization potential. Produces a scored candidate list with anti-pattern signals, ready to feed into /query-performance-deep-dive. Use for: account-wide optimization triage, finding high-impact queries to optimize, SE proactive account review, identifying recurring expensive patterns. Triggers: find queries to optimize, optimization candidates, account query audit, top queries to fix, what should I optimize, triage account, proactive optimization."
---

# Query Optimization Finder

Scan a customer's SNOWHOUSE query history (30-day default) to discover the top 10 query patterns most likely to benefit from deep-dive optimization. Each candidate is scored by a composite of **impact** (total compute burn) and **optimizability** (anti-pattern signals), producing a ranked list with credit cost estimates that feeds directly into `/query-performance-deep-dive`. Detects 11 anti-pattern types across stored procedures, DML, and SELECT queries. Supports single-account and multi-account (--org) modes.

## Prerequisites

- SNOWHOUSE access with SALES_ENGINEER role (or equivalent)
- SE_WH warehouse (or equivalent)
- Customer account locator
- The `/query-performance-deep-dive` skill installed (for follow-up deep dives)

**Load** `references/snowhouse-queries.md` for all query templates.
**Load** `references/scoring-criteria.md` for the scoring algorithm.

## Workflow

```
Step 1: Resolve Account (direct SNOWHOUSE lookup → Raven fallback for org aliases)
  ↓ ⚠️ STOP: confirm account
Step 2: Harvest Raw Candidates (30-day aggregation by parameterized hash, auto-exclusion)
  ↓
Step 3: Enrich Candidates (5 batched queries: scan/spill, SP children, WH size, table metadata, trend)
  ↓ ⚠️ STOP: present enrichment summary
Step 4: Score, Screen & Rank (composite scoring + anti-pattern mapping + credit cost estimation)
  ↓ ⚠️ STOP: present top-10 for approval
Step 5: Write Deliverables (ranked table + HTML report + table hotspot summary)
```

### Step 1: Resolve Account

**Goal:** Get account_id and deployment from locator or account alias.

The user may provide a **locator** (e.g., `FREDDIE_CNDLR`), an **account alias** (e.g., `ENT_PR`), or a **company/org name** (e.g., `Freddie Mac / FHLMC`). These are different identifiers:
- **Locator** = the `NAME` column in `ACCOUNT_ETL_V` (e.g., `FREDDIE_CNDLR`)
- **Account alias** = an org-level friendly name (e.g., `ENT_PR`) — NOT stored in `ACCOUNT_ETL_V`
- **Company/org name** = customer name (e.g., `Freddie Mac`) — stored in `COMPANY_NAME` in `ACCOUNT_ETL_V`

**Step 1a: Try direct SNOWHOUSE lookup first:**
```sql
USE WAREHOUSE SE_WH;
SELECT ID AS ACCOUNT_ID, NAME AS LOCATOR, DEPLOYMENT, COMPANY_NAME
FROM SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V
WHERE NAME = '<LOCATOR>';
```

**Step 1b: If no results (user gave an alias or org name), use Raven:**

Call the **Raven Account Lookup** tool with the user's input to resolve the alias to a locator:
```
Raven ACCOUNT_LOOKUP: "<company_name> <alias> account locator and deployment"
```
Raven returns `SNOWFLAKE_ACCOUNT_ALIAS`, `SNOWFLAKE_ACCOUNT_NAME` (= locator), and `SNOWFLAKE_DEPLOYMENT`. Use the `SNOWFLAKE_ACCOUNT_NAME` to look up `ACCOUNT_ID` in SNOWHOUSE.

**Step 1c: Confirm in SNOWHOUSE:**
```sql
SELECT ID AS ACCOUNT_ID, NAME AS LOCATOR, DEPLOYMENT
FROM SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V
WHERE NAME = '<LOCATOR_FROM_RAVEN>';
```

Save `ACCOUNT_ID`, `LOCATOR`, and `DEPLOYMENT`. All subsequent queries use these.

**⚠️ MANDATORY STOPPING POINT**: Confirm the resolved account with the user (show ACCOUNT_ID, LOCATOR, DEPLOYMENT, COMPANY_NAME) before proceeding to harvest.

**IMPORTANT:** Large enterprise customers (e.g., Freddie Mac, Disney) often have 100+ accounts across multiple deployments. The alias (e.g., `ENT_PR`) identifies a specific account within the org. Always use Raven when the user provides a name that doesn't directly match a SNOWHOUSE `NAME`.

### Step 2: Harvest Raw Candidates

**Goal:** Find all distinct query patterns (by `QUERY_PARAMETERIZED_HASH`) that ran in the last 30 days, aggregated to surface the most impactful patterns.

Use the **Candidate Harvest Query** from `references/snowhouse-queries.md`.

This query produces one row per parameterized hash with:
- `TOTAL_RUNS`: How many times this pattern executed
- `TOTAL_DURATION_MIN`: Cumulative runtime in minutes
- `AVG_DURATION_MIN`: Average runtime per execution
- `P95_DURATION_MIN`: 95th percentile runtime
- `MAX_DURATION_MIN`: Worst-case runtime
- `TOTAL_SCAN_TB`: Cumulative bytes scanned (TB)
- `TOTAL_SPILL_GB`: Cumulative spill (local + remote combined)
- `TOTAL_REMOTE_SPILL_GB`: Remote spill specifically (high penalty signal)
- `ERROR_COUNT`: Number of failed executions
- `SAMPLE_UUID_SLOWEST`: UUID of the slowest execution (for enrichment — worst-case signal detection)
- `SAMPLE_UUID_RECENT`: UUID of the most recent execution (for deep-dive commands and trend freshness)
- `SAMPLE_DESCRIPTION`: Truncated query text for identification
- `QUERY_TYPE`: Derived from DESCRIPTION prefix (CALL, MERGE, INSERT, UPDATE, DELETE, SELECT, COPY, CREATE, OTHER)
- `WAREHOUSE_NAME`: Primary warehouse used

**Filtering:**
- Exclude system/metadata queries (SHOW, DESCRIBE, EXPLAIN, USE, ALTER SESSION, GET, PUT, LIST, REMOVE)
- Exclude infrastructure queries: COMPUTE_SERVICE_WH, SYSTEM$CLUSTERING_WH warehouses; trust_center, SYSTEM$AUTO_REFRESH, replication/failover ALTER DATABASE, SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER
- Exclude queries under 10 seconds average (not worth deep-diving)
- Require at least 2 executions in the window (one-off queries are less impactful) — UNLESS a single execution exceeded 30 minutes

**Output limit:** Top 50 candidates by `TOTAL_DURATION_MIN` (we score and trim to 10 later).

### Step 3: Enrich Candidates

**Goal:** For each of the top 50 candidates, gather additional signals that inform scoring.

For each candidate hash, run the **Candidate Enrichment Query** from `references/snowhouse-queries.md` using `SAMPLE_UUID_SLOWEST`. This gathers:

**3a. SP Child Pattern Analysis** (for CALL types — Batch 2):
- Count of distinct child hashes
- Count of total children
- Whether any child hash repeats >5x (loop signal)
- Max single-child duration vs parent duration (bottleneck concentration)
- Whether children have children (nested SP depth)

**3b. Spill Profile** (Batch 1):
- Max single-execution spill (local + remote)
- Spill-to-scan ratio (% of scan that spills)
- Whether remote spill is present (boolean flag)

**3c. Scan Efficiency Signals** (Batch 1):
- `SCAN_INTENSITY_GBPM` = scan GB per minute of runtime (high value = scanning far more data than runtime justifies)
- `numPartitionsScanned / numPartitionsTotal` ratio (pruning effectiveness — often NULL in SNOWHOUSE, use fallback)
- `hashJoinNumberBroadcastDecisions` count (broadcast join signal)
- **NOTE:** `outputBytes` is always 0/NULL in SNOWHOUSE. Do NOT use `SCAN_TO_OUTPUT_RATIO`. Use `SCAN_INTENSITY_GBPM` as the proxy.

**3d. DML Signals** (Batch 1, for MERGE/INSERT/UPDATE/DELETE):
- `numRowsInserted` vs scan volume (fan-out detection)
- Whether MERGE scans >50 GB with no cluster key (unclustered target signal — see Step 3e)

**3e. Table Metadata Lookup** (Batch 4, for MERGE/DELETE/UPDATE candidates):
- Parse table name from DESCRIPTION
- Look up `CLUSTER_BY_KEYS` in `TABLE_ETL_V`
- Empty cluster key + high scan = strong unclustered target signal (Pattern 5/8)

**⚠️ MANDATORY STOPPING POINT**: Present enrichment summary (signal counts, data quality notes) to user before proceeding to scoring.

**3f. Warehouse Size Lookup** (Batch 3, for all candidates):
- Look up `SIZE` from `WAREHOUSE_ETL_V` for all distinct warehouses
- Map size to credits/hour: XS=1, S=2, M=4, L=8, XL=16, 2XL=32, 3XL=64, 4XL=128
- Remote spill on small warehouse = easy size-up win. Remote spill on large warehouse = data volume issue, optimize query instead.

**3g. Trend Signal** (Batch 5, **MANDATORY** for all 50 candidates — do not skip):
- Is this pattern DEGRADING (latest week >20% slower than prior week)?
- Is it STABLE-SLOW (consistent but expensive)?
- Is it INTERMITTENT (max > 3x median)?

**IMPORTANT:** Run enrichment as 5 batched queries (not per-candidate):
- Batch 1: Scan/spill/pruning for all 50 UUIDs (uses `SAMPLE_UUID_SLOWEST`)
- Batch 2: CALL child summary for all CALL UUIDs
- Batch 3: Warehouse size lookup for all distinct warehouses
- Batch 4: Table metadata for MERGE/DELETE/UPDATE candidates
- Batch 5: Trend analysis for all 50 hashes (**MANDATORY** — do not skip)

**Dual-UUID Strategy:** The harvest query captures two UUIDs per candidate:
- `SAMPLE_UUID_SLOWEST` — the execution with highest `TOTAL_DURATION` — used for enrichment queries (Batches 1-2) to detect worst-case anti-pattern signals
- `SAMPLE_UUID_RECENT` — the most recent execution — used in deep-dive commands (more likely to still be in query history cache) and for trend freshness checks

### Step 4: Score, Screen & Rank

**Goal:** Apply composite scoring, map anti-pattern signals, estimate credit costs, and produce a ranked top-10 list.

Use the scoring formula from `references/scoring-criteria.md`.

The score has two components:

**Impact Score (0-50 points):**
- Based on `TOTAL_DURATION_MIN`, `TOTAL_RUNS`, `TOTAL_SCAN_TB`, and `TOTAL_SPILL_GB`
- Weights: total cumulative runtime is the dominant factor (a 1-minute query running 10,000x/month is higher impact than a 60-min query running once)

**Optimizability Score (0-50 points):**
- Based on anti-pattern signal strength from Step 3
- Higher scores for patterns with clear, known fixes (e.g., cursor loop = very optimizable)
- Lower scores for patterns with ambiguous signals

**Final Score = Impact Score + Optimizability Score** (0-100)

Sort by descending score. Take top 10.

**Credit Cost Estimation:**
- `EST_CREDITS_30D = (TOTAL_DURATION_MIN / 60) * CREDITS_PER_HOUR` where `CREDITS_PER_HOUR` comes from the warehouse size lookup (Step 3f)
- `EST_SAVINGS_CREDITS = EST_CREDITS_30D * midpoint(EST_REDUCTION)` — estimated credit savings if the pattern is optimized

**Anti-Pattern Signal Mapping:** For each top-10 candidate, map enrichment signals to known patterns:

| Signal Combination | Suspected Pattern | Confidence |
|---|---|---|
| CALL with >100 same-hash children, 1 row each | Pattern 1: Cursor Loop | HIGH |
| CALL with >5 broadcast joins AND child scan >10 GB | Pattern 2: Missing Temp Stats | MEDIUM |
| Rows inserted >> rows after DISTINCT | Pattern 3: Join Explosion | MEDIUM |
| 10+ UPDATEs on same table | Pattern 4: Serial UPDATE Chain | MEDIUM |
| DELETE full-scans large table, no cluster key | Pattern 5: CDC DELETE Unclustered | HIGH |
| Many independent statements serial | Pattern 6: Serial Execution | MEDIUM |
| N identical CALLs, different filter only | Pattern 7: Repeated Identical Join | HIGH |
| MERGE full-scans despite selective ON clause, no cluster key | Pattern 8: MERGE Unclustered Target | HIGH |
| SCAN_GB > 100 on table with cluster key, query not leveraging it | Pattern 9: Full Table Scan | MEDIUM |
| SELECT * with >50 GB scan | Pattern 10: Expensive SELECT * | LOW |
| DESCRIPTION has 3+ UNION ALL with high scan | Pattern 11: UNION ALL Chain | MEDIUM |
| High spill-to-scan ratio (>50%) | Warehouse Undersized | HIGH |
| Remote spill > 0, warehouse SIZE ≤ Large | Warehouse Undersized (easy size-up) | HIGH |
| Remote spill > 0, warehouse SIZE ≥ 2X-Large | Data Volume Issue (optimize query) | MEDIUM |
| Degrading trend | Performance Regression | MEDIUM |
| No clear anti-pattern signals | Unknown — needs manual deep-dive | LOW |

For each candidate, assign:
- **Primary suspected pattern** (strongest signal)
- **Secondary signals** (other flags worth investigating)
- **Pre-screening confidence** (HIGH / MEDIUM / LOW / UNKNOWN)
- **Estimated optimization potential** (based on pattern's typical impact range from anti-patterns catalog)

**⚠️ MANDATORY STOPPING POINT**: Present the scored top-10 list with suspected patterns and credit estimates to user. Get approval before writing deliverables.

### Step 5: Write Deliverables

**Goal:** Produce three outputs:

#### 5a. Ranked Candidate Table (printed to console)

Print a formatted table with these columns:
```
| Rank | Score | Query Pattern | Type | Runs/30d | Total Min | Avg Min | P95 Min | Suspected Pattern | Confidence | Est. Reduction | Deep-Dive Command |
```

The `Deep-Dive Command` column contains the exact CoCo command to invoke the deep-dive skill:
```
/query-performance-deep-dive Analyze query <SAMPLE_UUID_RECENT> on account <LOCATOR>
```

#### 5b. HTML Report

Generate `<LOCATOR>_optimization_candidates.html` in the current working directory.

The HTML report includes:
1. **Executive Summary**: Account name, scan period, total compute burn, top optimization opportunity
2. **Candidate Cards** (one per candidate, ranked): Each card shows:
   - Rank badge and composite score
   - Query pattern snippet (first 200 chars of DESCRIPTION)
   - Key metrics: runs, total runtime, avg/p95/max, scan volume, spill
   - Anti-pattern signals with confidence badges
   - Trend indicator (DEGRADING / STABLE-SLOW / INTERMITTENT / HEALTHY)
   - Estimated savings range
   - Credit cost estimate (EST_CREDITS_30D) and potential savings
   - Warehouse size badge
   - One-click deep-dive command (copyable)
3. **Signal Heatmap**: Matrix of candidates × anti-pattern signals showing which patterns are most prevalent in this account
4. **Aggregate Statistics**: 
   - Total compute minutes scanned
   - Total estimated credits consumed (sum of EST_CREDITS_30D across top 10)
   - Potential credit savings estimate (sum of EST_SAVINGS_CREDITS)
   - Most common anti-pattern type in this account
   - Warehouse sizing recommendations summary

**HTML styling:** Use the same styling conventions as `/query-performance-deep-dive` output — clean, professional, Snowflake brand colors (#29B5E8 primary, #11567F dark, white background), monospace for SQL/code blocks.

#### 5c. Table Hotspot Summary

After scoring the top 10, aggregate scan volume by table name (parsed from DESCRIPTION) across all candidates. Print a mini-table:

```
| Table | Candidates Referencing | Total Scan TB | Suggestion |
```

Tables appearing in 3+ candidates are "hotspots" — clustering, materialized view, or partitioning candidates. Include this in the HTML report as a separate section after the candidate cards.

**How to parse table names:** Extract the first table reference after FROM, JOIN, MERGE INTO, DELETE FROM, UPDATE, or INSERT INTO in each candidate's SAMPLE_DESCRIPTION. Strip database/schema prefixes to normalize.

## Stopping Points

- ✋ After Step 1: Confirm resolved account (ACCOUNT_ID, LOCATOR, DEPLOYMENT) before harvesting
- ✋ After Step 3: Present enrichment summary (signal counts, any NULL/missing data) before scoring
- ✋ After Step 4: Present scored top-10 with patterns and credit estimates before writing deliverables

**Resume rule:** Upon user approval, proceed directly to the next step without re-asking.

## Output

- **Console table**: Ranked top-10 candidates with scores, patterns, and deep-dive commands
- **HTML report**: `<LOCATOR>_optimization_candidates.html` with candidate cards, signal heatmap, credit estimates, and table hotspot section
- **Table hotspot summary**: Tables referenced by 3+ candidates with clustering/partitioning suggestions

## Important Notes

### SNOWHOUSE Query Limits
- JOB_ETL_V does NOT have: EXECUTION_STATUS, STATUS, WAREHOUSE_SIZE, BYTES_SCANNED
- TABLE_ETL_V does NOT have: ROW_COUNT, BYTES
- WAREHOUSE_ETL_V: use SIZE not SIZE_ID
- STATS variant paths use `j.STATS:stats:<metric>::NUMBER` format
- QUERY_PARAMETERIZED_HASH groups structurally identical queries with different literal values

### Candidate Quality Over Quantity
- Better to return 5 strong candidates than 10 weak ones
- If fewer than 10 candidates have score > 30, truncate the list
- Always include at least 1 CALL-type candidate if any exist (SPs are the highest-value optimization targets)

### Handoff to Deep-Dive Skill
- Each candidate includes `SAMPLE_UUID_RECENT` (for deep-dive) and `SAMPLE_UUID_SLOWEST` (used during enrichment)
- The deep-dive command uses `SAMPLE_UUID_RECENT` — more likely to be in query history cache
- The deep-dive skill needs: UUID + account locator
- For SP patterns, the UUID should be the parent CALL, not a child
- For standalone DML, the UUID is the query itself

### Error Handling
- If account has < 100 queries in 30 days, note this as a low-activity account and adjust expectations
- If all queries are < 1 minute average, the account may not need optimization — report this finding
- If SNOWHOUSE deployment doesn't match expected pattern, try common deployments: `awsuswest2prod`, `azeastus2prod`, `awsuseast1prod`

### PARENT_JOB_ID Behavior
- In some deployments (e.g., `va2`), top-level queries have `PARENT_JOB_ID = 0` instead of `NULL`
- Always use `(PARENT_JOB_ID IS NULL OR PARENT_JOB_ID = 0)` to catch both cases
- Child queries have non-zero `PARENT_JOB_ID` pointing to the parent's `JOB_ID`

## Multi-Account Mode (--org)

When the user specifies `--org` or provides an org name without a specific account alias:

1. **List accounts:** Use Raven Account Lookup to list all accounts for the org
2. **Lightweight harvest:** For each account, run a simplified harvest query (top 5 by total duration only — no enrichment)
3. **Rank accounts:** Sort accounts by total compute burn (sum of TOTAL_DURATION_MIN across top 5)
4. **Full analysis:** Run the complete 6-step workflow on the top N accounts (default: 3)
5. **Combined report:** Produce a single HTML report with per-account sections, plus a cross-account summary showing:
   - Which accounts are most expensive
   - Common anti-patterns across accounts
   - Shared table hotspots (same table scanned heavily in multiple accounts)

**Note:** Multi-account mode reuses all existing query templates — it simply loops the workflow per account. The lightweight harvest in step 2 uses the same harvest query but with `LIMIT 5` and no enrichment, purely for account ranking.
