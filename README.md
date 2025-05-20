# CustomerInsight360
An advanced SQL-based data integration project that builds a unified 360° view of customer behavior and conversions for an online retailer using MS SQL Server.

## 📊 Project Overview

CustomerInsight360 is an advanced SQL-based data integration project that builds a 360-degree view of customer behavior for an online retail platform. The project integrates multiple fact and dimension tables to deliver a unified customer profile including conversion activity, order history, and cumulative revenue insights.

Developed using Microsoft SQL Server, this project was part of the MMAI 5100 curriculum at York University’s Schulich School of Business and demonstrates advanced SQL techniques including CTEs, joins, window functions, and schema design.

---

## 🧠 Objectives

- Create a comprehensive `Customer360` view from multiple source tables.
- Link static conversion data with dynamic weekly order history.
- Track customer activity across conversions and cumulative revenue.
- Handle weeks without orders via outer joins and null-safe logic.

---

## 📁 Data Sources

The following tables from the `mmai_db` database were used:

- `fact_tables.orders`
- `fact_tables.conversions`
- `dimensions.date_dimension`
- `dimensions.product_dimension`
- `dimensions.customer_dimension`

---

## 🏗️ View Structure: `customer360.customer_360_view`

The final view contains the following grouped data:

### 🎯 Static Conversion Data
- `customer_id`, `first_name`, `last_name`
- `conversion_id`, `conversion_number`, `conversion_type`
- `conversion_date`, `conversion_week`, `next_conversion_week`
- `conversion_channel`

### 🛍️ First Order Data
- `first_order_week`, `first_order_total_paid`

### 📆 Weekly Order History
- `week_counter`, `order_week`, `orders_placed` (binary)
- `total_before_discounts`, `total_discounts`, `total_paid_in_week`
- `conversion_cumulative_revenue`, `lifetime_cumulative_revenue`

---

## 🔁 Process Summary

1. **Customer/Conversion Join**  
   Used `INNER JOIN` to combine customers with conversions and date dimension, using ranking to compute conversion number per customer.

2. **Next Conversion Matching**  
   Used `LEFT JOIN` to find the week of the next conversion (if any), per customer.

3. **First Order Lookup**  
   Extracted the total paid and week from the first order linked to each conversion.

4. **Order History Aggregation**  
   Grouped orders by week using the `date_dimension`, then joined to conversion data.

5. **Week Generation & Expansion**  
   Used `CROSS JOIN` to generate all week combinations per conversion between current and next conversion.

6. **Join Order History with Weekly Structure**  
   Merged with order history using `LEFT JOIN` to preserve weeks with no orders, replacing NULLs with 0s using `COALESCE`.

7. **Cumulative Metrics**  
   Used `SUM(...) OVER(...)` window functions to compute cumulative revenue metrics.

---

## ⚙️ Technologies Used

- **SQL Server (MSSQL)**
- **CTEs (Common Table Expressions)**
- **Window Functions**
- **Joins (INNER, LEFT, CROSS)**
- **Schema Creation (`customer360`)**

---

## 🚧 Challenges & Solutions

- Used `running_week` to align conversions with order dates.
- Handled null values using `COALESCE` to ensure full weekly coverage.
- Cumulative revenue calculation moved to final step to account for weeks without orders.
- Verified data accuracy using `INTERSECT` and `EXCEPT` SQL tests.

---
## 👨‍💻 Contributors

- Sahand Farhangi  
- Narotam Dhaliwal  
- Warisha Saad  
- Alekhya Erikipati  

---
## 🏷️ Tags

`SQL` `Customer360` `Data Warehousing` `MSSQL` `ETL` `CTE` `Analytics` `Retail` `Window Functions`
