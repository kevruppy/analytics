WITH CTE AS (
	SELECT
		_CHECK_SUM
		,PRODUCT_NAME
		,START_DATE
		,END_DATE
		,BASE_PROVISION_VALUE
	FROM
		{{ ref('cre_provision_rules_all') }}
	{% if is_incremental() %}
		WHERE
			TRUE
			AND
			_CHECK_SUM NOT IN (SELECT DISTINCT TGT._CHECK_SUM FROM {{ this }} AS TGT)
	{% endif %}
)

SELECT
	_CHECK_SUM
	,PRODUCT_NAME
	,START_DATE
	,END_DATE
	,BASE_PROVISION_VALUE
FROM
	CTE
