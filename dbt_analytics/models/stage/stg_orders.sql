WITH CTE_ORDERS AS (
	SELECT
		JSON_EXTRACT_STRING(
			LOAD_RESULT,['ORDER_ID','STATUS_NAME','PRODUCT_NAME','CREATION_DATE','STATUS_CHANGE_DATE','IS_TEST_ORDER']
		) AS _EXTRACT
	FROM
		{{ source('raw', 'orders') }}
	{% if is_incremental() %}
		WHERE
			TRUE
			AND
			_EXTRACT[4]::DATE > (SELECT MAX(TGT.CREATION_DATE) - 60 AS WATERMARK FROM {{ this }} AS TGT)
	{% endif %}
)

,CTE_ORDERS_CASTED AS (
	SELECT
		_EXTRACT[1]::INTEGER AS ORDER_ID
		,_EXTRACT[2]::VARCHAR AS STATUS_NAME
		,_EXTRACT[3]::VARCHAR AS PRODUCT_NAME
		,_EXTRACT[4]::DATE AS CREATION_DATE
		,_EXTRACT[5]::DATE AS STATUS_CHANGE_DATE
		,_EXTRACT[6]::BOOLEAN AS IS_TEST_ORDER
	FROM
		CTE_ORDERS
)

SELECT
	ORDER_ID
	,STATUS_NAME
	,PRODUCT_NAME
	,CREATION_DATE
	,STATUS_CHANGE_DATE
	,IS_TEST_ORDER
FROM
	CTE_ORDERS_CASTED
