SELECT
	_CHECK_SUM
	,PRODUCT_NAME
	,START_DATE
	,END_DATE
	,BASE_PROVISION_VALUE
FROM
	{{ ref('core','cre_provision_rules') }}
{% if is_incremental() %}
	WHERE
		_CHECK_SUM NOT IN (SELECT DISTINCT _CHECK_SUM FROM {{ this }})
{% endif %}
