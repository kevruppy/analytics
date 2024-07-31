WITH CTE AS (
    SELECT
        ord.* 
    FROM {{ ref('int_orders_gross_net_agg_per_month') }} AS ORD
INNER JOIN
	{{ ref('cre_provision_rules_proportional') }} AS PROV
ON
	ORD.PRODUCT_NAME = PROV.PRODUCT_NAME
AND
	ORD.CREATION_MONTH BETWEEN PROV.START_DATE AND PROV.END_DATE
and 
cnt_ORDERS > PROV.PROPORTIONAL_PROVISION_TARGET_VALUE
)