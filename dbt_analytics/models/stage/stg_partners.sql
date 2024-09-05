WITH CTE_PARTNERS AS (
	SELECT
		UPDATED_ON
		,PRODUCT_NAME
		,PARTNER_NAME
		,{{ dbt_utils.generate_surrogate_key(['UPDATED_ON','PRODUCT_NAME']) }} AS HASH_KEY --noqa
	FROM
		{{ source('raw', 'partners') }}
)

SELECT
	HASH_KEY
	,UPDATED_ON
	,PRODUCT_NAME
	,PARTNER_NAME
FROM
	CTE_PARTNERS
