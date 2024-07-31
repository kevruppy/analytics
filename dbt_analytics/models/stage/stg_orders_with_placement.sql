SELECT ORDER_ID
FROM
	{{ source('raw', 'orders_with_placement') }}
{% if is_incremental() %}
	WHERE
		ORDER_ID NOT IN (SELECT DISTINCT ORDER_ID FROM {{ this }})
{% endif %}
