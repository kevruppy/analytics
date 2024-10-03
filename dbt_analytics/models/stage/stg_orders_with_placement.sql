WITH CTE AS (
	SELECT (LOAD_RESULT ->> 'ORDER_ID')::INTEGER AS ORDER_ID
	FROM
		{{ source('raw', 'orders_with_placement') }}
	{% if is_incremental() %}
		WHERE
			TRUE
			AND
			(LOAD_RESULT ->> 'ORDER_ID')::INTEGER NOT IN (SELECT DISTINCT TGT.ORDER_ID FROM {{ this }} AS TGT)
	{% endif %}
)

SELECT ORDER_ID
FROM
	CTE
