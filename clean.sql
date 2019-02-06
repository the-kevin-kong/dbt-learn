with orders as (

    select * from source_data.orders 
    where orders.state != 'canceled' 
    and extract(year from orders.completed_at) < '2018' 
    and orders.email not like '%company.com' 

)

, items as (

    select * from source_data.order_items

)

, order_agg as (

    select 
        orders.id
        , orders.number
        , orders.completed_at
        , orders.completed_at::date as completed_at_date
        , sum(orders.total) as net_rev
        , sum(orders.item_total) as gross_rev
        , count(orders.id) as order_count
    from source_data.orders
    where net_rev >= 150000
    group by 1,2,3,4

)

, orders_complete as (

    select 
    
        order_items.order_id
        , orders.completed_at::date as completed_at_date
        , sum(order_items.quantity) as qty 

    from items
    left join orders using (order_id)
    where orders.is_cancelled_order = false 
    OR orders.is_pending_order != true 
    group by 1,2

)

select 
    
    a.completed_at_date
    , a.gross_rev
    , a.net_rev
    , b.qty
    , a.order_count as orders
    , b.qty/a.distinct_orders as avg_unit_per_order
    , a.Gross_Rev/a.distinct_orders as aov_gross
    , a.Net_Rev/a.distinct_orders as aov_net

from a 
join b 
on a.completed_at_date = b.completed_at_date
order by completed_at_date desc