# Merge Flow Analysis — Data Sources & Join Logic

This document traces the data merge flow for **2026 (current year)** and **2023–2025 (archive years)**, and identifies potential issues.

---

## 1. Overview by Year

| Year | Source(s) | Merge Location | Output |
|------|-----------|----------------|--------|
| **2026** | `6w_v3.RDS` (single file) | None — already merged by planning team | Loaded directly; `compute_avl_corr_sales` adds GSheet lookups only |
| **2023–2025** | Sales.RDS + Forecast.RDS + Avl.RDS | `merge_archive_rds.py` | `merged_2023/2024/2025.parquet` |

---

## 2. 2026 Flow (6w_v3.RDS)

**Source:** Single RDS file from `04_6w_rolling_data/6w_v3.RDS`

**Assumed structure:** Pre-merged by the planning team. Contains:
- Sales: city_name, hub_name, product_id, process_dt, sales, revenue, sub_category, product_name
- Forecast: r7_plan, r7_inv, group_flag, group_instances, etc.
- Availability: flag, instances (at product or group level)

**Our pipeline:**
1. Load RDS (or parquet if converted)
2. `compute_avl_corr_sales` merges with:
   - **P_Master** (product_id → Anchor ID, Anchor Name)
   - **Avl_Flag** (product_id → SKU Class Prod, Avl Flag)
   - **Subcat-Type** (sub_category → Type)
   - **SellThroughFactor** (Type × day × hour × city → salethroughfactor)

**Potential issues:**
- If 6w_v3 has different column names or granularity than expected, merges may fail or produce NaN
- No visibility into how 6w_v3 was originally built — schema may drift

---

## 3. Archive Flow (2023–2025) — merge_archive_rds.py

### 3.1 Input Files

| File | Granularity | Key Columns |
|------|-------------|-------------|
| **Sales** | product × hub × day | city_name, hub_name, process_dt, product_id, product_name, sub_category, sales, revenue |
| **Forecast** | product × hub × day | city_name, hub_name, **date** (→ process_dt), product_id, r7_plan, r7_inv, group_flag, group_instances |
| **Availability** | **sku_group** × hub × day | city_name, hub_name, **av_dt**, **sku_group**, flag, instances |

### 3.2 Join Logic

**Step 1: SKU Class Prod mapping (for Avl join)**
- P Master: `product_id` → `SKU Class Prod`
- cc cat (fallback): `product_name` → `SKU Class Prod`
- Set `sales["sku_group"] = sales["SKU Class Prod"]`

**Step 2: Sales + Forecast**
- Join keys: `(city_name, hub_name, process_dt, product_id)`
- Forecast uses `date` column → normalized to `process_dt`
- Adds: r7_plan, r7_inv, group_flag, group_instances, etc.

**Step 3: (Sales+Forecast) + Availability**
- Join keys: `(city_name, hub_name, process_dt, av_dt, sku_group)`
- Sales left: `sku_group` = SKU Class Prod from P Master
- Avl right: `sku_group` = native column in Availability file

---

## 4. Critical Issue: sku_group Mismatch

### 4.1 What the merge assumes

The merge assumes:
```
Sales.sku_group (from P Master SKU Class Prod)  ==  Avl.sku_group
```

### 4.2 What the sample data shows

From `merge_samples/`:

**Availability sku_group** (sample values):
- "Parshe Whole, Cleaned"
- "Double Yolk Eggs - Pack of 6"
- "Chicken Curry Cut with Skin - Small Pieces"
- "Chicken Seekh Kebab"
- "Lucknowi Mutton Galouti Kebab"

These look like **product names** or **product-level identifiers**.

**P Master SKU Class Prod** (from data_loader / Avl_Flag usage):
- Typically **cut classifications** (e.g. "Chicken - CC", "Chicken - BL", "Fish - Whole")
- Maps product_id → a category/cut type

**Sales product_name** (sample):
- "Chicken Curry Cut (Large - 8 to 10 Pieces)"
- "Chunky Butter Chicken Spread (Single Serve)"

### 4.3 Conclusion

If **Avl.sku_group** = product names and **P Master SKU Class Prod** = cut classifications, then:
- `sales.sku_group` (from P Master) will **not** match `avl.sku_group`
- The Availability merge will produce **mostly NaN** for flag/instances
- `unmapped_rows_log.txt` and `merge_debug_report.txt` would show high Avl missing %

**To verify:** Run `merge_archive_rds.py` and check:
1. `unmapped_rows_log.txt` — unmapped SKU (P Master / cc cat)
2. `merge_debug_report.txt` — "Rows missing Avl (flag/instances)" count

### 4.4 Possible fixes

| Option | Description |
|-------|-------------|
| **A. Use product_name for Avl join** | If Avl.sku_group = product_name, join on `product_name` instead of SKU Class Prod. Requires: `sales.merge(avl, left_on=[..., "product_name"], right_on=[..., "sku_group"])` |
| **B. Separate Avl sku_group mapping** | Create a mapping table: product_id → Avl_sku_group (if different from P Master). Load from a sheet or derive from Avl’s distinct sku_groups. |
| **C. Confirm schema with data owners** | Confirm whether Avl.sku_group is meant to align with P Master SKU Class Prod, product_name, or another key. |

---

## 5. Other Potential Issues

### 5.1 Forecast date column

- 2023, 2025 Forecast: have `date`, not `process_dt` → script normalizes `date` → `process_dt` ✓
- 2024 Forecast: merge_sample_summary reported memory error when reading — large file; may need chunked read or more RAM

### 5.2 Forecast file naming

- 2025: `97Forecast.RDS` (different name)
- 2023, 2024: `Forecast.RDS`

### 5.3 P Master column scope

- `merge_archive_rds` uses `max_cols=10` for P Master (columns A:J) to avoid duplicate "Product id"
- Only uses `Product id`, `SKU Class Prod` from P Master
- `data_loader` uses `Product id`, `Anchor ID`, `Anchor Name` — different columns, same sheet

### 5.4 sub_category source

- Sales has `sub_category`
- Merge output must have `sub_category` for downstream (Subcat-Type, level grouping)
- Currently carried from Sales ✓

---

## 6. Downstream Expectations (data_loader)

After merge, `compute_avl_corr_sales` expects:

| Column | Source (archive) | Source (2026) |
|--------|------------------|---------------|
| city_name, hub_name, product_id, process_dt | Sales | 6w |
| sales, revenue | Sales | 6w |
| sub_category, product_name | Sales | 6w |
| sku_group | From P Master (as SKU Class Prod) | 6w |
| flag, instances | Availability merge | 6w |
| group_flag, group_instances, r7_* | Forecast merge | 6w |

For archive, if the Avl merge fails (sku_group mismatch), `flag` and `instances` will be NaN. The pipeline then uses `product_level_avl = 1.0` (no correction) when flag/instances are missing — so results are wrong but it does not crash.

---

## 7. Recommended Next Steps

1. **Run merge and inspect outputs**
   - `py scripts/merge_archive_rds.py`
   - Check `unmapped_rows_log.txt` and `merge_debug_report.txt`

2. **Compare sku_group values**
   - From Sales (after P Master): distinct `SKU Class Prod` values
   - From Avl: distinct `sku_group` values
   - Check overlap and whether they use the same ontology

3. **If sku_group ≠ SKU Class Prod**
   - Try joining Avl on `product_name` instead of `sku_group`
   - Or get a mapping from data owners

4. **Document 6w_v3 schema**
   - Inspect columns and sample values
   - Confirm it matches what `compute_avl_corr_sales` expects
