# model desc

{% docs dsc_int_orders_gross_net_agg_per_month %}
Aggregation of orders per order type, creation month and product name
{% enddocs %}

# cols desc

{% docs dsc_order_type %}
Either 'GROSS_ORDER' (all orders) or 'NET_ORDER' (only confirmed orders)
{% enddocs %}

{% docs dsc_creation_month %}
Month of order creation (truncated to first of month)
{% enddocs %}

{% docs dsc_cnt_orders %}
Count of orders for particular grouping set
{% enddocs %}
