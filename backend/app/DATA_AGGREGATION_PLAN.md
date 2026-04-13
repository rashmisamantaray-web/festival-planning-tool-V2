# Data Aggregation Plan

Based on **Data Aggregation.docx**.

## Context

- **6w_v3.RDS** (current 2026 source) only has data from Mid November onward.
- Data for **2023, 2024, 2025** is split across separate files:
  - **Sales data** → `Sales.RDS`
  - **Availability data** → `SKU_Class_Avl_8am_8pm.RDS`
- **Forecast data** → `Forecast.RDS` (provides r7_plan, grp_r7_*, group_flag, etc.)
- Need to **read**, **merge** and produce one combined file per year.

## File Locations

| Year | Sales Data | Availability Data | Forecast Data |
|------|------------|-------------------|---------------|
| 2025 | `03_archive_data/Sales.RDS` | `03_archive_data/SKU_Class_Avl_8am_8pm.RDS` | `03_archive_data/97Forecast.RDS` |
| 2024 | `03_archive_data/2024/Sales.RDS` | `03_archive_data/2024/SKU_Class_Avl_8am_8pm.RDS` | `03_archive_data/2024/Forecast.RDS` |
| 2023 | `03_archive_data/2023/Sales.RDS` | `03_archive_data/2023/SKU_Class_Avl_8am_8pm.RDS` | `03_archive_data/2023/Forecast.RDS` |

Base path:  
`G:\.shortcut-targets-by-id\1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em\Planning Team\25. Planning_Database\01_all_day_reporting\`

**Forecast.RDS** provides the planning columns (r7_plan, r7_inv, grp_r7_*, group_flag, group_instances, BasePlan) required by the availability-correction pipeline. Merged on (city_name, hub_name, process_dt, product_id) with Sales.

## Execution Plan

### Phase 1: Read & Inspect (first)

1. Script to read all 6 RDS files.
2. List for each file:
   - Columns and dtypes
   - Row count
   - Sample values for key join columns (hub, date, product_id)
3. Produce a comparison report:
   - Non-uniform column names across years
   - Data type mismatches
   - Missing columns vs `RDS_COLUMNS_TO_KEEP`

### Phase 2: Merge (implemented)

1. **SKU Class Prod mapping** (two-step):
   - Primary: P Master (Product id → SKU Class Prod)
   - Fallback: cc cat sheet (unique product name → SKU Class Prod) for unmapped rows
   - Whitespace trimmed on product names for robust fallback matching
2. **Column normalization**: `hub_id` coerced to string; `process_dt` / `av_dt` to datetime.
3. Per year: **merge Sales + Forecast** on `(city_name, hub_name, process_dt, product_id)` → adds r7_plan, grp_r7_*, group_flag, group_instances, BasePlan, etc.
4. Per year: **merge Sales (with Forecast) + Availability** on `(city_name, hub_name, process_dt=av_dt, sku_group)`.
5. **Unmapped logging**: counts logged to console and `unmapped_rows_log.txt` in output dir.
6. Output: `merged_2023.parquet`, `merged_2024.parquet`, `merged_2025.parquet` in `.../01. Forecasting/Weekly Forecasts/2026/Historicals Festive Tool/`.

## Expected Downstream Columns

The current pipeline (`data_loader.py`) expects these columns from the merged RDS:

```
city_name, product_id, hub_name, sku_group, process_dt,
sales, revenue, product_discount,
group_flag, group_instances,
grp_r7_plan, grp_r7_inv, grp_r7_plan_rev, grp_r7_inv_rev,
grp_BasePlan, grp_BaseRev,
r7_plan, r7_inv, r7_plan_rev, r7_inv_rev,
BasePlan, flag, instances, sub_category, product_name
```

Phase 1 will reveal which of these (if any) come from Sales vs Availability and what renames are needed.
