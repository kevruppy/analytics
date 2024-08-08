WITH CTE_ORD_GROSS_PROV AS (
	SELECT
		ORD.ORDER_ID
		,ORD.IS_CONFIRMED
		,PROV_BASE.BASE_PROVISION_VALUE AS GROSS_BASE_PROVISION
		,CASE WHEN ORD.IS_PLACEMENT = TRUE THEN PROV_PLACE.PLACEMENT_PROVISION_VALUE END AS GROSS_PLACEMENT_PROVISION
	FROM
		{{ ref('int_orders_status_dates') }} AS ORD
	-- ALL ORDERS MUST HAVE A BASE COMMISSION
	INNER JOIN
		{{ ref('cre_provision_rules_base') }} AS PROV_BASE
		ON
			ORD.PRODUCT_NAME = PROV_BASE.PRODUCT_NAME
			AND
			ORD.CREATION_DATE BETWEEN PROV_BASE.START_DATE AND PROV_BASE.END_DATE
	-- ORDERS MIGHT HAVE A PLACEMENT PROVISION
	LEFT OUTER JOIN
		{{ ref('cre_provision_rules_placement') }} AS PROV_PLACE
		ON
			ORD.PRODUCT_NAME = PROV_PLACE.PRODUCT_NAME
			AND
			ORD.CREATION_DATE BETWEEN PROV_PLACE.START_DATE AND PROV_PLACE.END_DATE
)

,CTE_ORD_GROSS_NET_PROV AS (
	SELECT
		ORDER_ID
		-- GROSS
		,GROSS_BASE_PROVISION
		,COALESCE(GROSS_PLACEMENT_PROVISION,0) AS GROSS_PLACEMENT_PROVISION
		-- NET
		,CASE
			WHEN IS_CONFIRMED = TRUE THEN GROSS_BASE_PROVISION ELSE 0
		END AS NET_BASE_PROVISION
		,CASE
			WHEN IS_CONFIRMED = TRUE THEN GROSS_PLACEMENT_PROVISION ELSE 0
		END AS NET_PLACEMENT_PROVISION
	FROM
		CTE_ORD_GROSS_PROV
)

SELECT
	ORDER_ID
	,GROSS_BASE_PROVISION
	,GROSS_PLACEMENT_PROVISION
	,NET_BASE_PROVISION
	,NET_PLACEMENT_PROVISION
FROM
	CTE_ORD_GROSS_NET_PROV
