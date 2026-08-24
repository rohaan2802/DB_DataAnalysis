# DB_DataAnalysis

Database coursework: **ER modeling (draw.io)**, **T-SQL** on an **Olist-style ECOMMERCE** schema (bulk load + cleaning + analysis), exported CSVs/PNGs, plus **NorthWind** practice files.

**Roll:** i222327 · [rohaan2802](https://github.com/rohaan2802)

---

## Table of contents

1. [Assignment #01 — modeling](#assignment-01--modeling)
2. [Assignment #02 — ECOMMERCE SQL](#assignment-02--ecommerce-sql)
3. [Tables and constraints](#tables-and-constraints)
4. [Bulk insert](#bulk-insert)
5. [Analysis outputs](#analysis-outputs)
6. [How to run](#how-to-run)
7. [NorthWind](#northwind)

---

## Assignment #01 — modeling

- `DB_Assignment#1.pdf` brief  
- `i222327_DB_Assignment#01.docx`  
- `i222327_*_sec_A.drawio` + PNG exports  

Open `.drawio` in [diagrams.net](https://app.diagrams.net/).

---

## Assignment #02 — ECOMMERCE SQL

**Script:** `Assignment #02_SQL.sql` (local copy `Assignment02.sql`)

Creates database **`ECOMMERCE`**, then Task 1 **upload + cleaning**, then analytical `SELECT`s. Result CSVs/PNGs sit under `CUSTOMER ANALYSIS/` (and order/product sibling folders).

Olist CSVs originally bulk-loaded from a machine path:

`C:\Users\ALLEN PROGRAMMER\Downloads\olist_*_dataset.csv`

**Replace every `BULK INSERT` path** before executing on your PC.

---

## Tables and constraints

Order of creation matters (FKs). Observed objects:

| Table | Keys / CHECKs |
|-------|----------------|
| `Geolocation` | Referenced by customers/sellers (composite zip+city+state) |
| `Customers` | PK `customer_id` non-empty; unique id; zip `> 0`; FK geolocation **ON DELETE CASCADE** |
| `Orders` | PK `order_id`; status **IN** `delivered, shipped, processing, canceled, unavailable, invoiced, created, approved`; nullable timestamps; CHECKs that approval ≥ purchase, carrier ≥ approval, customer delivery after carrier, estimate ≥ purchase |
| `Order_Payments` | PK `(order_id, payment_sequential)`; types `credit_card, boleto, voucher, debit_card, not_defined`; installments `> 0`; value `≥ 0` |
| `Products` | PK `product_id`; FK category translation; photos qty 0–50; dimensions `> 0` where set. Column name **`product_catery_name`** (typo vs “category”) matches the Olist dump |
| `Sellers` | PK `seller_id`; FK geolocation |
| `Order_Items` | line items: `price`, `freight_value` ≥ 0, `shipping_limit_date` |

Further tables (reviews, translations, etc.) continue in the rest of the SQL file — execute the full script.

`CODEPAGE = '65001'` (UTF-8), `KEEPNULLS`, `TABLOCK` on several bulk loads. Orders bulk insert also uses UTF-8; Customers insert in the extract may omit CODEPAGE — align if you see character errors.

---

## Bulk insert pattern

```sql
BULK INSERT Orders
FROM 'D:\data\olist_orders_dataset.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    CODEPAGE = '65001',
    KEEPNULLS,
    TABLOCK
);
```

`GO` batches are required in SSMS.

---

## Analysis outputs

After DDL/DML, run the analytical section and compare to:

- `CUSTOMER ANALYSIS/*.csv` and `*.png`  
- Order / product analysis folders in `Assignment_2_i222327/`

Typical questions: orders by status, payment mix, revenue by state, delivery delay vs estimate, product category sales.

---

## How to run

SQL Server Express/Developer + SSMS or `sqlcmd`:

```bash
sqlcmd -S .\SQLEXPRESS -E -i "Assignment #02/Assignment_2_i222327/Assignment #02_SQL.sql"
```

1. Fix paths.  
2. Run Geolocation (and translation tables) **before** Customers/Sellers/Products.  
3. Only then run analysis `SELECT`s.

---

## NorthWind

`NorthWind DataBase/` — extra classic schema practice, separate from Olist.

Olist / NorthWind retain upstream licenses.

---

## Author

**i222327** · [rohaan2802](https://github.com/rohaan2802)
