SELECT (LOAD_RESULT ->> 'ORDER_ID')::INTEGER AS ORDER_ID
FROM
	{{ source('raw', 'orders_with_placement') }}
{% if is_incremental() %}
	WHERE
		(LOAD_RESULT ->> 'ORDER_ID')::INTEGER NOT IN (SELECT DISTINCT ORDER_ID FROM {{ this }})
{% endif %}
