# Execution Plan: Archive Merge & Integration

## 1. Output Path Change

**New output directory:**
```
G:\.shortcut-targets-by-id\1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em\Planning Team\01. Forecasting\Weekly Forecasts\2026\Historicals Festive Tool
```

**Files written:**
- `merged_2023.parquet`
- `merged_2024.parquet`
- `merged_2025.parquet`
- `unmapped_rows_log.txt`

---

## 2. Do We Need r7_plan, grp_r7_plan, etc.?

**Short answer: No, for the archive merged data.**

| Column | In 6w_v3 (2026) | In Archive Merged | Needed for Revenue? |
|--------|-----------------|-------------------|---------------------|
| sales | ✅ | ✅ | Raw input |
| revenue | ✅ | ✅ | Raw input |
| flag | ✅ | ✅ (from Avl) | Availability fraction |
| instances | ✅ | ✅ (from Avl) | Availability fraction |
| r7_plan, r7_inv, r7_plan_rev | ✅ | ❌ | Used for *weighting* availability |
| group_flag, group_instances | ✅ | ❌ | Used when plan_sum=0 |
| grp_r7_* | ✅ | ❌ | Same as above |

**Current pipeline logic (6w_v3):**
- Uses r7_inv → plan_sum (anchor-level inventory)
- Uses r7_plan_rev to *weight* flag/instances (planned revenue as weight)
- product_level_avl = weighted_flag / weighted_instances
- Avl_Corr_Revenue = revenue / salethroughfactor

**For archive data we can simplify:**
- Avl has flag and instances at sku_group level (already merged)
- Use: **product_level_avl = flag / instances** (direct, no r7 weighting)
- **Avl_Corr_Revenue = revenue / salethroughfactor** (unchanged)
- Result: Revenue-based availability correction without r7/grp columns

---

## 3. What the Merged Parquet Will Contain

| Source | Columns |
|--------|---------|
| Sales | city_name, hub_id, hub_name, product_id, product_name, category, sub_category, process_dt, sales, revenue, product_discount, ... |
| P Master + cc cat | sku_group (added) |
| Availability | flag, instances |

**Not included:** r7_plan, grp_r7_plan, group_flag, group_instances, etc.

---

## 4. Execution Steps

### Step A: Update merge script output path
- Change `OUTPUT_DIR` in `merge_archive_rds.py` to the new path.

### Step B: Run merge script
```powershell
cd c:\Users\Rashmi\Documents\festival-planning-tool\backend
py scripts\merge_archive_rds.py
```

### Step C: Integrate with data_loader (separate phase)
- Add config for archive parquet paths (2023, 2024, 2025).
- In `load_rds_data` or equivalent: when loading 2023–2025, read from parquet instead of RDS.
- Add a **simplified** `compute_avl_corr_sales` path for archive data:
  - Skip plan_sum, group_flag logic.
  - Use: `product_level_avl = flag / instances` (fillna 0).
  - Rest of pipeline (Type, OOS time, SellThroughFactor, Avl_Corr_Revenue) unchanged.

### Step D: Wire RDS_PATHS
- Add 2023, 2024, 2025 entries in config pointing to the merged parquet files.
- Keep 2026 as 6w_v3.RDS (full pipeline).

---

## 5. Summary Checklist

| # | Task | Owner |
|---|------|-------|
| 1 | Update OUTPUT_DIR in merge_archive_rds.py | Done in this change |
| 2 | Run merge script (G: drive + credentials) | User |
| 3 | Review unmapped_rows_log.txt | User |
| 4 | Add archive parquet config | Future |
| 5 | Add simplified compute_avl_corr_sales for archive | Future |
| 6 | Add 2023–2025 to RDS_PATHS | Future |

---

## 6. Order of Operations

1. **Now:** Change output path, run merge → get parquet files.
2. **Later:** Modify data_loader to support archive parquet + simplified Avl logic.
3. **Later:** Update RDS_PATHS so Compute uses 2023–2025 merged data.
