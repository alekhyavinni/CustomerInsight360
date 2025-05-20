/*
--- GROUP 7 ---
Sahand Farhangi
Alekhya Erikipati
Narotam Dhaliwal
Warisha Saad
*/

USE sahand1_db;
---------
-- Scheme creation
CREATE SCHEMA customer360;
DROP SCHEMA customer360;
---------
-- customer360 query
SELECT *
FROM customer360.customer360
ORDER BY customer_id, conversion_number;

DROP VIEW customer360.customer360;
---------
-- create customer360
CREATE VIEW customer360.customer360 AS
    WITH customer_conversions AS (
        SELECT sk_customer, customer_id, first_name, last_name, conversion_id,
               RANK() OVER (PARTITION BY fk_customer ORDER BY conversion_date) AS conversion_number,
               conversion_type, conversion_date, year_week AS conversion_week, conversion_channel,
               order_number AS first_order_number, running_week AS running_conversion_week
        FROM (mmai_db.dimensions.customer_dimension
            INNER JOIN mmai_db.fact_tables.conversions ON sk_customer = fk_customer)
            INNER JOIN mmai_db.dimensions.date_dimension ON fk_conversion_date = sk_date
    ),

    find_next_conversion AS (
        SELECT orig.sk_customer AS sk_customer,
               orig.customer_id AS customer_id,
               orig.first_name AS first_name,
               orig.last_name AS last_name,
               orig.conversion_id AS conversion_id,
               orig.conversion_number AS conversion_number,
               orig.conversion_type AS conversion_type,
               orig.conversion_date AS conversion_date,
               orig.conversion_week AS conversion_week,
               orig.conversion_channel AS conversion_channel,
               orig.first_order_number AS first_order_number,
               orig.running_conversion_week AS running_conversion_week,
               findnext.customer_id AS id2, findnext.conversion_number AS cn2,
               findnext.conversion_week AS next_conversion_week
        FROM customer_conversions orig
            LEFT JOIN customer_conversions findnext
                ON orig.customer_id = findnext.customer_id
                    AND orig.conversion_number + 1 = findnext.conversion_number
    ),

    customer_conversions_first_order AS (
        SELECT sk_customer, customer_id, first_name, last_name, conversion_id,conversion_number,
            conversion_type, conversion_date, conversion_week, conversion_channel, next_conversion_week,
            running_conversion_week, year_week AS first_order_week, price_paid AS first_order_total_paid
        FROM find_next_conversion
            INNER JOIN mmai_db.fact_tables.orders ON first_order_number = order_number
            INNER JOIN mmai_db.dimensions.date_dimension ON fk_order_date = sk_date
    ),

    order_data AS (
        SELECT year_week AS order_week, fk_customer,
               running_week AS running_order_week,
               SUM(unit_price) AS total_before_discounts,
               SUM(discount_value) AS total_discounts,
               SUM(price_paid) AS total_paid_in_week
        FROM mmai_db.fact_tables.orders
            INNER JOIN mmai_db.dimensions.date_dimension ON fk_order_date = sk_date
        GROUP BY fk_customer, year_week, running_week
    ),

    order_history_1 AS (
        SELECT sk_customer, customer_id, first_name, last_name, conversion_id,conversion_number,
               conversion_type, conversion_date, conversion_week, conversion_channel, next_conversion_week,
               first_order_week, first_order_total_paid,
               order_week, total_before_discounts, total_discounts, total_paid_in_week,
               (running_order_week - running_conversion_week) AS week_since_conversion
        FROM customer_conversions_first_order
            INNER JOIN order_data ON fk_customer = sk_customer
        WHERE order_data.order_week >= conversion_week
    ),

    rank_order_history_1 AS (
        SELECT sk_customer, customer_id, first_name, last_name, conversion_id,conversion_number,
           conversion_type, conversion_date, conversion_week, conversion_channel, next_conversion_week,
           first_order_week, first_order_total_paid,
           order_week, total_before_discounts, total_discounts, total_paid_in_week,
           RANK() OVER (PARTITION BY customer_id, order_week ORDER BY week_since_conversion ASC) rankNum
        FROM order_history_1
    ),

    order_history_2 AS (
        SELECT sk_customer, customer_id, first_name, last_name, conversion_id,conversion_number,
               conversion_type, conversion_date, conversion_week, conversion_channel, next_conversion_week,
               first_order_week, first_order_total_paid,
               order_week, 1 AS orders_placed, total_before_discounts, total_discounts, total_paid_in_week
        FROM rank_order_history_1
        WHERE rankNum = 1
    ),

    find_distinct_weeks AS (
        SELECT DISTINCT(year_week) AS distinct_week
        FROM mmai_db.dimensions.date_dimension
    ),

    combine_distinct_weeks AS (
        SELECT customer_id, first_name, last_name,
               conversion_id, conversion_number, conversion_type, conversion_date, conversion_week,
               conversion_channel, first_order_week, first_order_total_paid,
               next_conversion_week, distinct_week
        FROM customer_conversions_first_order CROSS JOIN find_distinct_weeks
        WHERE distinct_week >= conversion_week AND (distinct_week < next_conversion_week OR (next_conversion_week IS NULL))
    ),

    create_week_counter AS (
        SELECT customer_id, first_name, last_name,
               conversion_id, conversion_number, conversion_type, conversion_date, conversion_week,
               conversion_channel, first_order_week, next_conversion_week, first_order_total_paid,
               RANK() OVER (PARTITION BY conversion_id ORDER BY distinct_week ASC) AS week_counter,
               distinct_week AS order_week
        FROM combine_distinct_weeks
    ),

    combine_all_weeks AS (
        SELECT cwc.customer_id AS customer_id,
               cwc.first_name AS first_name,
               cwc.last_name AS last_name,
               cwc.conversion_id AS conversion_id,
               cwc.conversion_number AS conversion_number,
               cwc.conversion_type AS conversion_type,
               cwc.conversion_week AS conversion_week,
               cwc.conversion_channel AS conversion_channel,
               cwc.conversion_date AS conversion_date,
               cwc.next_conversion_week AS next_conversion_week,
               cwc.first_order_week AS first_order_week,
               cwc.first_order_total_paid AS first_order_total_paid,
               cwc.week_counter AS week_counter,
               cwc.order_week AS order_week,
               COALESCE(oh2.orders_placed, 0) AS orders_placed,
               COALESCE(oh2.total_before_discounts, 0.00) AS total_before_discounts,
               COALESCE(oh2.total_discounts, 0.00) AS total_discounts,
               COALESCE(oh2.total_paid_in_week, 0.00) AS total_paid_in_week
        FROM create_week_counter AS cwc LEFT JOIN order_history_2 AS oh2
            ON cwc.conversion_id = oh2.conversion_id
                   AND cwc.order_week = oh2.order_week
    )
    SELECT customer_id, first_name, last_name,
           conversion_id, conversion_number, conversion_type, conversion_date, conversion_week, conversion_channel,
           next_conversion_week, first_order_week, first_order_total_paid,
           week_counter, order_week, orders_placed, total_before_discounts, total_discounts, total_paid_in_week,
           SUM(total_paid_in_week)
               OVER (PARTITION BY conversion_id ORDER BY order_week ASC) conversion_cumulative_revenue,
           SUM(total_paid_in_week)
               OVER (PARTITION BY customer_id ORDER BY order_week ASC) lifetime_cumulative_revenue
    FROM combine_all_weeks;
--------------------------


------- TESTING ----------

SELECT customer_id, first_name, last_name,
       conversion_id, conversion_number, conversion_type, conversion_date, conversion_week, conversion_channel,
       next_conversion_week, first_order_week, first_order_total_paid,
       week_counter, order_week, order_placed, total_before_discounts, total_discounts, total_paid,
       conversion_cum_revenue, lifetime_cum_revenue
FROM mmai_db.customer360.conversions_with_order_history
INTERSECT
SELECT * FROM customer360.customer360;
-- returns 749 rows, same size as both tables

SELECT customer_id, first_name, last_name,
       conversion_id, conversion_number, conversion_type, conversion_date, conversion_week, conversion_channel,
       next_conversion_week, first_order_week, first_order_total_paid,
       week_counter, order_week, order_placed, total_before_discounts, total_discounts, total_paid,
       conversion_cum_revenue, lifetime_cum_revenue
FROM mmai_db.customer360.conversions_with_order_history
EXCEPT
SELECT * FROM customer360.customer360;
-- returns no rows: all rows in example are contained in customer360

SELECT customer_id, first_name, last_name,
       conversion_id, conversion_number, conversion_type, conversion_date, conversion_week, conversion_channel,
       next_conversion_week, first_order_week, first_order_total_paid,
       week_counter, order_week, order_placed, total_before_discounts, total_discounts, total_paid,
       conversion_cum_revenue, lifetime_cum_revenue
FROM mmai_db.customer360.conversions_with_order_history
EXCEPT
SELECT * FROM customer360.customer360;
-- returns no rows: all rows in customer360 contained in example