# Scoring Criteria — Query Optimization Finder

## Composite Score Formula

Each candidate receives a score from 0 to 100, composed of two equal-weight components:

```
FINAL_SCORE = IMPACT_SCORE (0-50) + OPTIMIZABILITY_SCORE (0-50)
```

---

## Impact Score (0-50 points)

Measures how much compute this pattern burns and how frequently it runs. A query that runs 10,000 times at 1 minute each is far more impactful than a 60-minute query that runs once.

### Sub-components

| Component | Max Points | Metric | Logic |
|-----------|-----------|--------|-------|
| Cumulative Runtime | 25 | `TOTAL_DURATION_MIN` | Log-scaled: `MIN(25, ROUND(LN(TOTAL_DURATION_MIN + 1) * 5, 0))` |
| Frequency | 10 | `TOTAL_RUNS` | `MIN(10, ROUND(LN(TOTAL_RUNS + 1) * 2.5, 0))` |
| Scan Volume | 10 | `TOTAL_SCAN_TB` | `MIN(10, ROUND(TOTAL_SCAN_TB * 20, 0))` |
| Spill Penalty | 5 | `TOTAL_SPILL_GB` | `MIN(5, ROUND(TOTAL_SPILL_GB / 100, 0))` + 2 bonus if any remote spill |

### Log-scale rationale
Linear scoring would be dominated by extreme outliers. Log-scaling ensures:
- A 10-minute total and a 100-minute total are meaningfully differentiated
- A 10,000-minute total doesn't swamp everything else
- Frequency matters: 1000 runs scores much higher than 10 runs, but 10,000 vs 5,000 is less dramatic

### Example calculations:
| Pattern | TOTAL_MIN | RUNS | SCAN_TB | SPILL_GB | Remote? | Impact Score |
|---------|-----------|------|---------|----------|---------|-------------|
| SP running 400x/day, 5 min avg | 60,000 | 12,000 | 2.5 | 500 | Yes | 25+10+10+5+2 = **50** |
| MERGE running 1x/day, 30 min | 900 | 30 | 0.3 | 50 | No | 17+9+6+1 = **33** |
| SELECT running 100x/day, 2 min | 6,000 | 3,000 | 0.1 | 0 | No | 22+10+2+0 = **34** |
| One-off 45-min query | 45 | 1 | 0.05 | 10 | No | 10+2+1+0 = **13** |

---

## Optimizability Score (0-50 points)

Measures how likely this query is to have a known fix based on anti-pattern signals.

### Signal-Based Scoring

Each detected signal contributes points. Signals can stack, but the total is capped at 50.

| Signal | Points | Condition | Maps to Pattern |
|--------|--------|-----------|-----------------|
| **Strong loop detected** | 20 | CALL with >100 same-hash children, avg ≤5 rows each | Pattern 1: Cursor Loop |
| **Weak loop detected** | 10 | CALL with 20-100 same-hash children | Pattern 1 (lower confidence) |
| **Broadcast join excess** | 12 | `TOTAL_BROADCAST_JOINS > 5` AND `TOTAL_CHILD_SCAN_GB > 10` on children. If broadcasts >5 but scan <10 GB: 3 pts (likely correct optimizer behavior) | Pattern 2: Missing Temp Stats |
| **Fan-out detected** | 15 | `ROWS_INSERTED > SCAN_GB * 10M` (output >> input rows) | Pattern 3: Join Explosion |
| **Serial UPDATE chain** | 10 | 10+ UPDATE children on same hash pattern | Pattern 4: Serial UPDATE |
| **Full table scan DELETE** | 12 | DELETE with `PRUNING_RATIO_PCT > 90` or (when NULL: SCAN_GB > 10 AND no cluster key from TABLE_ETL_V) | Pattern 5: CDC DELETE |
| **Serial independent work** | 8 | CALL with >10 children, each unique hash, similar durations | Pattern 6: Serial Execution |
| **Repeated identical join** | 12 | CALL with >5 children sharing same hash, different params | Pattern 7: Repeated Join |
| **MERGE full scan** | 15 | MERGE with `PRUNING_RATIO_PCT > 90` or (when NULL: SCAN_GB > 50 AND no cluster key from TABLE_ETL_V). With cluster key but high scan: 8 pts | Pattern 8: MERGE Unclustered |
| **Remote spill present** | 8 | `REMOTE_SPILL_GB > 0`. +3 bonus if warehouse SIZE ≤ Large (easy size-up). -3 if SIZE ≥ 2X-Large (data volume issue) | Warehouse undersized |
| **Heavy spill** | 5 | Combined spill > 50% of scan | Warehouse undersized |
| **Degrading trend** | 5 | Latest week >20% slower than prior week | Performance regression |
| **High scan intensity** | 5 | `SCAN_INTENSITY_GBPM > 100` (scanning >100 GB/min on queries >5 min avg) | General inefficiency |
| **CALL type bonus** | 3 | Query is a stored procedure (higher optimization leverage) | SP complexity |
| **Full table scan heuristic** | 8 | SELECT/DML with SCAN_GB > 100 + table has cluster key from TABLE_ETL_V but query likely not leveraging it (MEDIUM confidence) | General inefficiency |
| **Expensive SELECT *** | 5 | DESCRIPTION starts with `SELECT *` AND SCAN_GB > 50 | Column over-fetch |
| **UNION ALL chain** | 10 | DESCRIPTION contains 3+ occurrences of `UNION ALL` AND SCAN_GB > 50 (repeated full scans) | Serial full scans |

### Scoring rules:
1. Apply all matching signals and sum
2. Cap at 50
3. If no anti-pattern signals are detected (score = 0), assign 5 points as baseline (unknown patterns may still be optimizable upon deep-dive)

### Confidence mapping:
- **Optimizability ≥ 30**: HIGH confidence — strong anti-pattern signals, known fix path
- **Optimizability 15-29**: MEDIUM confidence — partial signals, needs deep-dive to confirm
- **Optimizability 5-14**: LOW confidence — weak signals, optimization potential uncertain
- **Optimizability < 5**: UNKNOWN — no recognizable pattern, manual inspection required

---

## Ranking and Cutoff

1. Sort all 50 candidates by `FINAL_SCORE` descending
2. Take the top 10
3. If fewer than 10 candidates score above 30, only return those above 30
4. **Exception:** Always include at least 1 CALL-type candidate if any exist (SPs have disproportionate optimization leverage)
5. **Exception:** Always include any candidate with `OPTIMIZABILITY_SCORE >= 30` regardless of rank (strong anti-pattern signals should never be buried)

---

## Output Columns

The final ranked table includes:

| Column | Description |
|--------|-------------|
| `RANK` | 1-10 position |
| `SCORE` | Composite score (0-100) |
| `IMPACT` | Impact sub-score (0-50) |
| `OPTIMIZABILITY` | Optimizability sub-score (0-50) |
| `QUERY_TYPE` | CALL, MERGE, INSERT, UPDATE, DELETE, SELECT, etc. |
| `PATTERN` | Truncated query pattern (first 80 chars) |
| `RUNS_30D` | Total executions in window |
| `TOTAL_MIN` | Cumulative runtime (minutes) |
| `AVG_MIN` | Average per-execution runtime |
| `P95_MIN` | 95th percentile runtime |
| `SCAN_TB` | Total scan volume |
| `SPILL_GB` | Total spill (local + remote) |
| `SUSPECTED_PATTERN` | Primary anti-pattern (or "Unknown") |
| `CONFIDENCE` | HIGH / MEDIUM / LOW / UNKNOWN |
| `TREND` | DEGRADING / STABLE-SLOW / INTERMITTENT / HEALTHY |
| `EST_REDUCTION` | Estimated runtime reduction range (e.g., "70-95%") |
| `DEEP_DIVE_CMD` | `/query-performance-deep-dive Analyze query <SAMPLE_UUID_RECENT> on account <LOCATOR>` |
| `WH_SIZE` | Warehouse size from WAREHOUSE_ETL_V (e.g., Medium, Large) |
| `EST_CREDITS_30D` | Estimated credits consumed in 30 days (TOTAL_MIN/60 * credits_per_hour) |

---

## Estimated Reduction Ranges by Pattern

| Pattern | Typical Reduction | Range |
|---------|------------------|-------|
| Pattern 1: Cursor Loop | 99%+ | 95-99.9% |
| Pattern 2: Missing Temp Stats | 60-80% | 50-90% |
| Pattern 3: Join Explosion | 90-99% | 80-99% |
| Pattern 4: Serial UPDATE Chain | 30-60% | 20-70% |
| Pattern 5: CDC DELETE Unclustered | 70-90% | 60-95% |
| Pattern 6: Serial Execution | 50-80% | 30-90% |
| Pattern 7: Repeated Identical Join | 80-90% | 70-95% |
| Pattern 8: MERGE Unclustered Target | 70-95% | 60-95% |
| Warehouse Undersized (spill) | 30-50% | 20-60% |
| SELECT * Over-fetch | 20-50% | 10-60% |
| UNION ALL Chain | 50-80% | 40-90% |
| Unknown (manual deep-dive) | Varies | 10-50% |
