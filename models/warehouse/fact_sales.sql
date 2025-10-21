--partition is done to speed up the query & will save cost because fact tables are usually very large
--it is applied on a column which is used very frequently by business users
--to apply partition, we need to do write a 'configuration' function before our CTE statement. Make sure the column used in
-----config has a data_type and same data type is casted in the CTE query

-----PARTITIONING----
{{
    config(
        partition_by = {
            "field" : "order_date",
            "data_type" : "date"
        }
    )
}}

---CTE-----
with source as(
    select
        od.order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        od.quantity,
        od.unit_price,
        od.discount,
        od.status_id,
        od.date_allocated,
        od.purchase_order_id,
        od.inventory_id,
        date(o.order_date) as order_date,
        o.shipped_date,
        o.paid_date,
        current_timestamp() as insertion_timestamp
    from {{ ref('stg_orders')}} o left join {{ ref('stg_order_details')}} od on o.id = od.order_id
    where od.order_id is not null
),

-- TO REMOVE DUPLICATE ROWS ---
unique_source as (
    select *,
            row_number() over(partition by customer_id, employee_id, order_id, product_id, shipper_id, purchase_order_id, shipper_id, order_date) as row_number
    from source
)
select * 
except
       (row_number),
from unique_source
where row_number = 1