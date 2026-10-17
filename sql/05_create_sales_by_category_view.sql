CREATE OR REPLACE VIEW thornquist_db.v_sales_by_category AS
SELECT 
    category,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS total_items_sold,
    ROUND(SUM(unit_price * quantity), 2) AS total_revenue
FROM thornquist_db.curated_sales
GROUP BY category;