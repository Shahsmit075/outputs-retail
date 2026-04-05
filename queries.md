### 🔹 Cetas_apr2026_loaded [(output)](./cetas_apr2026_loaded.csv)
```sql
SELECT *
FROM gold.sales_apr2026_summary
ORDER BY total_revenue DESC;
````

---

### 🔹 [Cross-Table Analytical Query](./Cross-Table%20Analytical%20Query.csv)

```sql
-- Which product sells best in which city?
SELECT
   p.product,
   c.city,
   p.total_revenue   AS product_revenue,
   c.total_revenue   AS city_revenue,
   ROUND(
       p.total_revenue * 100.0 / SUM(p.total_revenue) OVER(), 1
   ) AS pct_of_total_revenue
FROM      gold.vw_sales_by_product  p
CROSS JOIN gold.vw_sales_by_city    c
ORDER BY  product_revenue DESC;
```

---

### 🔹 [Date-partitioned Query](./Date-partitioned%20query.csv)

```sql
SELECT
   product,
   city,
   COUNT(*)          AS total_orders,
   SUM(total_amount) AS total_revenue,
   AVG(total_amount) AS avg_order_value
FROM OPENROWSET(
   BULK        'silver/sales_clean/',
   DATA_SOURCE = 'lake_adls',
   FORMAT      = 'DELTA'
) AS r
WHERE ingested_at >= '2026-04-01'
  AND ingested_at <  '2026-05-01'
GROUP BY product, city
ORDER BY total_revenue DESC;
```

---

### 🔹 [High Value Order Analysis](./High%20Value%20Order%20Analysis.csv)

```sql
SELECT
   city,
   product,
   COUNT(*)          AS high_value_orders,
   SUM(total_amount) AS high_value_revenue
FROM OPENROWSET(
   BULK        'silver/sales_clean/',
   DATA_SOURCE = 'lake_adls',
   FORMAT      = 'DELTA'
) AS r
WHERE total_amount > 40000
GROUP BY city, product
ORDER BY high_value_revenue DESC;
```

---

### 🔹 [Hourly Trend Query](./Hourly%20Trend%20Query.csv)

```sql
SELECT
   LEFT(CAST(ingested_at AS VARCHAR(50)), 13) AS hour_bucket,
   COUNT(*)                                   AS orders_in_hour,
   SUM(total_amount)                          AS revenue_in_hour
FROM OPENROWSET(
   BULK        'silver/sales_clean/',
   DATA_SOURCE = 'lake_adls',
   FORMAT      = 'DELTA'
) AS r
GROUP BY LEFT(CAST(ingested_at AS VARCHAR(50)), 13)
ORDER BY hour_bucket;
```

---

### 🔹 [Sales by City](./Sales%20by%20city.csv)

```sql
-- Sales by city
SELECT *
FROM OPENROWSET(
   BULK        'sales_summary/by_city/',
   DATA_SOURCE = 'gold_adls',
   FORMAT      = 'DELTA'
) AS city_summary
ORDER BY total_revenue DESC;
```

---

### 🔹 [Sales by Payment Method](./Sales%20by%20payment%20method.csv)

```sql
-- Sales by payment method
SELECT *
FROM OPENROWSET(
   BULK        'sales_summary/by_payment/',
   DATA_SOURCE = 'gold_adls',
   FORMAT      = 'DELTA'
) AS payment_summary
ORDER BY total_orders DESC;
```

---

### 🔹 [Sales by Product](./Sales%20by%20product.csv)

```sql
SELECT *
FROM OPENROWSET(
   BULK        'sales_summary/by_product/',
   DATA_SOURCE = 'gold_adls',
   FORMAT      = 'DELTA'
) AS r
ORDER BY total_revenue DESC;
```
