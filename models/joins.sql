with prod as (
    select
        c.category_name,
        s.company_name suppliers,
        p.product_name,
        p.unit_price,
        p.product_id
    from {{source("sources","products")}} p
    left join {{source("sources","suppliers")}} s on s.supplier_id=p.supplier_id
    left join {{source("sources","categories")}} c on c.category_id=p.category_id
), orddetai as (
    select
        od.order_id,
        od.quantity,
        od.discount,
        pr.*
    from {{ref("orderdetails")}} od
    left join prod pr on pr.product_id=od.product_id
), ord as (
    select
        o.order_date,
        o.order_id,
        c.company_name customer,
        e.name employee,
        e.age,
        e.lengthofservice
    from {{source("sources","orders")}} o
    left join {{ref("customers")}} c on o.customer_id=c.customer_id
    left join {{ref("employees")}} e on o.employee_id=e.employee_id
    left join {{source("sources","shippers")}} s on o.ship_via=s.shipper_id
), finaljoin as (
    select
        o.order_date,
        o.customer,
        o.employee,
        o.age,
        o.lengthofservice,
        od.*
    from orddetai od
    inner join ord o on od.order_id=o.order_id
) select * from finaljoin
