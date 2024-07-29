SELECT
	_CHECK_SUM
	,PRODUCT_NAME
	,START_DATE
	,END_DATE
	,BASE_PROVISION_VALUE
FROM
  {{ ref('core','cre_provision_rules') }}