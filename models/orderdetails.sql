select
    od.order_id,
    od.product_id,
    od.unit_price,
    od.quantity,
    p.product_name,
    p.supplier_id,
    p.category_id,
    od.unit_price*od.quantity total,
    (p.unit_price * od.quantity) - total discount
from {{source("sources","order_details")}} od
left join {{source("sources","products")}} p on od.product_id=p.product_id
