WITH CTE AS (
	SELECT
		_CHECK_SUM
		,PRODUCT_NAME
		,START_DATE
		,END_DATE
		,PLACEMENT_PROVISION_VALUE
	FROM
		{{ ref('cre_provision_rules') }}
	WHERE
		PLACEMENT_PROVISION_VALUE IS NOT NULL
		{% if is_incremental() %}
			AND
			_CHECK_SUM NOT IN (SELECT DISTINCT _CHECK_SUM FROM {{ this }})
		{% endif %}
)

SELECT
	_CHECK_SUM
	,PRODUCT_NAME
	,START_DATE
	,END_DATE
	,PLACEMENT_PROVISION_VALUE
FROM CTE
