WITH CTE_PROVISION_RULES AS (
	SELECT
		_CHECK_SUM
		,PRODUCT_NAME
		,START_DATE
		,END_DATE
		,BASE_PROVISION AS BASE_PROVISION_VALUE
		,PLACEMENT_PROVISION AS PLACEMENT_PROVISION_VALUE
		,(PROPORTIONAL_PROVISION ->> 'target')::INTEGER AS PROPORTIONAL_PROVISION_TARGET_VALUE
		,(PROPORTIONAL_PROVISION ->> 'unit')::VARCHAR AS PROPORTIONAL_PROVISION_TARGET_UNIT
		,(PROPORTIONAL_PROVISION ->> 'value')::FLOAT AS PROPORTIONAL_PROVISION_VALUE
	FROM
		{{ ref('stg_provision_rules') }}
	QUALIFY
		ROW_NUMBER() OVER (PARTITION BY _CHECK_SUM ORDER BY 1) = 1
)

,CTE_PROVISION_RULES_ENRICHED AS (

	SELECT
		PROV_RULES._CHECK_SUM
		,MAP_PRODUCT.PRODUCT_NAME
		,PROV_RULES.START_DATE
		,PROV_RULES.END_DATE
		,PROV_RULES.BASE_PROVISION_VALUE
		,PROV_RULES.PLACEMENT_PROVISION_VALUE
		,PROV_RULES.PROPORTIONAL_PROVISION_TARGET_VALUE
		,PROV_RULES.PROPORTIONAL_PROVISION_TARGET_UNIT
		,PROV_RULES.PROPORTIONAL_PROVISION_VALUE
	FROM CTE_PROVISION_RULES AS PROV_RULES
	INNER JOIN {{ ref('map_provision_rules_product') }} AS MAP_PRODUCT
		ON PROV_RULES.PRODUCT_NAME = MAP_PRODUCT.PROVISION_RULES_PRODUCT_NAME
)

SELECT
	_CHECK_SUM
	,PRODUCT_NAME
	,START_DATE
	,END_DATE
	,BASE_PROVISION_VALUE
	,PLACEMENT_PROVISION_VALUE
	,PROPORTIONAL_PROVISION_TARGET_VALUE
	,PROPORTIONAL_PROVISION_TARGET_UNIT
	,PROPORTIONAL_PROVISION_VALUE
FROM
	CTE_PROVISION_RULES_ENRICHED
