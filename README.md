# DB_DataAnalysis

Database coursework spanning **ER modeling (draw.io)**, **T-SQL schema + analysis queries**, and exported **customer / order / product analysis CSVs**. Built around an Olist-style e-commerce schema (`ECOMMERCE`) plus NorthWind materials.

**Author roll referenced in files:** i222327

---

## Overview

| Area | Contents |
|------|----------|
| **Assignment #01** | PDF brief, Word solution, ER diagrams (`.drawio` + PNG exports) |
| **Assignment #02** | Full SQL script: create DB/tables, bulk load CSVs, cleaning, analytical queries; result CSVs under CUSTOMER / ORDER / PRODUCT ANALYSIS |
| **NorthWind DataBase** | Additional NorthWind-related database practice assets |

Assignment #02's `Assignment #02_SQL.sql` creates `ECOMMERCE`, defines `Customers`, `Orders`, and related tables with CHECKs/FKs, uses `BULK INSERT` from Olist CSVs, then runs analysis tasks whose outputs are saved as CSV screenshots/exports.

---

## Features

- Constrained DDL (non-empty IDs, order status enums, geolocation FKs)
- Documented `BULK INSERT` patterns for CSV onboarding
- Analysis folders with CSV results and PNG query evidence:
  - `CUSTOMER ANALYSIS/`
  - Order and product analysis counterparts inside the Assignment #02 tree
- Visual ERDs for early modeling tasks

---

## Repository structure

```text
DB_DataAnalysis/
├── Assignment #01/
│   ├── DB_Assignment#1.pdf
│   ├── i222327_DB_Assignment#01.docx
│   └── i222327_*_sec_A.drawio[.png]
├── Assignment #02/
│   ├── Assignment_2_i222327.zip
│   └── Assignment_2_i222327/
│       ├── Assignment #02_SQL.sql
│       ├── CUSTOMER ANALYSIS/*.csv | *.png
│       └── ... (order/product analysis exports)
└── NorthWind DataBase/
```

---

## Build / run

Requires **Microsoft SQL Server** (Express/Developer) and **SSMS** or `sqlcmd`.

1. Adjust `BULK INSERT` paths in `Assignment #02_SQL.sql` to your local Olist CSV locations (the script contains an author machine path - replace it).
2. Execute batches in order (respect `GO` separators):

```sql
-- In SSMS: open Assignment #02_SQL.sql and Execute
-- Or:
sqlcmd -S .\SQLEXPRESS -E -i "Assignment #02/Assignment_2_i222327/Assignment #02_SQL.sql"
```

3. Open Assignment #01 `.drawio` files in [diagrams.net](https://app.diagrams.net/) for ER review.

---

## Usage

- Run DDL/DML sections first; only then execute analytical `SELECT`s.
- Compare query outputs to CSVs under `CUSTOMER ANALYSIS/` (and sibling folders).
- Use Assignment #01 diagrams when documenting schema rationale in reports.

---

## Extending

- Parameterize bulk-load paths via SQLCMD variables.
- Add indexed views for frequent customer/order aggregations.
- Port key analyses to a Jupyter + pandas notebook reading the same CSVs.
- Normalize geolocation handling if source CSV quality varies.

---

## License

Academic database coursework - Olist/NorthWind datasets retain their upstream licenses.
