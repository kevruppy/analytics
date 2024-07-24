WITH CTE AS (
	SELECT
		JSON_EXTRACT_STRING(
			LOAD_RESULT,['ORDER_ID','STATUS_NAME','PRODUCT_NAME','CREATION_DATE','STATUS_CHANGE_DATE','IS_TEST_ORDER']
		) AS _EXTRACT
	FROM
		{{ source('raw', 'orders') }}
	{% if is_incremental() %}
		WHERE
			_EXTRACT[5]::DATE > (SELECT MAX(STATUS_CHANGE_DATE) AS WATERMARK FROM {{ this }})
	{% endif %}
)

SELECT
	_EXTRACT[1]::INTEGER AS ORDER_ID
	,_EXTRACT[2]::VARCHAR AS STATUS_NAME
	,_EXTRACT[3]::VARCHAR AS PRODUCT_NAME
	,_EXTRACT[4]::DATE AS CREATION_DATE
	,_EXTRACT[5]::DATE AS STATUS_CHANGE_DATE
	,_EXTRACT[6]::BOOLEAN AS IS_TEST_ORDER
FROM
	CTE
