SELECT
	TRANSACTION_ID
	,RATING_DATE
	,RATING
	,TOOL
FROM
	{{ source('raw', 'net_promotor_scores') }}
{% if is_incremental() %}
	WHERE
		TRANSACTION_ID NOT IN (SELECT DISTINCT TRANSACTION_ID FROM {{ this }})
{% endif %}
