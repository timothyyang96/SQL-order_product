with 
order_category as (
    /* This CTE retrieves order details along with the total bill amount and product category name */
    select
        oit.order_id
        , (price + freight_value) as total_bill /* Calculating the total bill as the sum of price and freight value */
        , pdc.product_category_name /* Getting the product category name */
        , ord.order_purchase_timestamp /* Getting the order purchase timestamp */
    from 
        brooklyndata.olist_order_items_dataset as oit
    inner join 
        brooklyndata.olist_products_dataset as pdc
    on 
        oit.product_id = pdc.product_id
    inner join 
        brooklyndata.olist_orders_dataset as ord
    on 
        oit.order_id = ord.order_id
)
, order_revenue as (
    /* This CTE calculates daily revenue metrics including the total and average revenue per order */
    select
        cast(order_purchase_timestamp as date) as order_purchase_date /* Converting timestamp to date */
        , count(distinct ord.order_id) as orders_count /* Counting the distinct number of orders */
        , count(distinct customer_id) as customers_making_orders_count /* Counting the distinct number of customers making orders */
        , cast(sum(payment_value) as numeric(10, 2)) as revenue_usd /* Calculating the total revenue in USD */
        , cast(sum(payment_value) / count(distinct ord.order_id) as numeric(10, 2)) as average_revenue_per_order_usd /* Calculating the average revenue per order in USD */
    from 
        brooklyndata.olist_orders_dataset as ord
    inner join 
        brooklyndata.olist_order_payments_dataset as pay
    on 
        ord.order_id = pay.order_id
    group by 
        order_purchase_date
)
, date_category_rev as (
    /* This CTE calculates the daily revenue generated from each product category */
    select
        cast(order_purchase_timestamp as date) as order_purchase_date /* Converting timestamp to date */
        , product_category_name /* Getting the product category name */
        , sum(total_bill) as category_revenue /* Calculating the total revenue generated from each category */
    from 
        order_category
    group by 
        order_purchase_date
        , product_category_name
)
, ranked_category as (
    /* This CTE ranks the product categories based on the percentage of total revenue they contribute to on a daily basis */
    select
        dcr.order_purchase_date
        , product_category_name
        , cast(category_revenue / revenue_usd as numeric(10, 2)) as percentage /* Calculating the percentage of total revenue contributed by each category */
        , rank() over (
            partition by dcr.order_purchase_date
            order by category_revenue / revenue_usd desc
        ) as ranking /* Ranking the categories based on the revenue percentage */
    from 
        date_category_rev as dcr
    inner join 
        order_revenue as rev
    on 
        dcr.order_purchase_date = rev.order_purchase_date
)
, top_3_category_percentage as (
    /* This CTE retrieves the top 3 product categories by revenue percentage for each day */
    select
        order_purchase_date
        , string_agg(coalesce(product_category_name, ''), ', ') as top_3_product_categories_by_revenue /* Aggregating the top 3 product categories by revenue */
        , string_agg(coalesce(cast(percentage as text), '0'), ', ') as top_3_product_categories_revenue_percentage /* Aggregating the revenue percentage of the top 3 product categories */
    from 
        ranked_category
    where 
        ranking <= 3
    group by 
        order_purchase_date
)

/* This final query combines the data from the above CTEs to produce the final report */
select
    rev.order_purchase_date
    , orders_count
    , customers_making_orders_count
    , revenue_usd
    , average_revenue_per_order_usd
    , top_3_product_categories_by_revenue
    , top_3_product_categories_revenue_percentage
from 
    order_revenue as rev
inner join 
    top_3_category_percentage as top
on 
    rev.order_purchase_date = top.order_purchase_date;
