--noqa: disable=AM04,RF02,RF04

WITH TEST_ORDERS_PREP AS (
	SELECT
		(LOAD_RESULT ->> 'ORDER_ID')::INTEGER AS ORDER_ID
		,(LOAD_RESULT ->> 'IS_TEST_ORDER')::BOOLEAN AS IS_TEST_ORDER
	FROM
		{{ source('raw', 'orders') }}
)

,TEST_ORDERS AS (
	SELECT ORDER_ID
	FROM
		TEST_ORDERS_PREP
	GROUP BY ALL
	HAVING
		CASE WHEN LIST_CONTAINS(ARRAY_AGG(DISTINCT IS_TEST_ORDER),TRUE) THEN TRUE ELSE FALSE END = TRUE --noqa: disable=ST02
)

,RAW_ORDERS AS (
	SELECT
		JSON_EXTRACT_STRING(
			LOAD_RESULT
			,['ORDER_ID','STATUS_NAME','PRODUCT_NAME','CREATION_DATE','STATUS_CHANGE_DATE','IS_TEST_ORDER']
		) AS _EXTRACT
	FROM
		{{ source('raw', 'orders') }}
)

,ORDERS_PLACEMENT AS (
	SELECT (LOAD_RESULT ->> 'ORDER_ID')::INTEGER AS ORDER_ID
	FROM
		{{ source('raw', 'orders_with_placement') }}
)

,ORDERS AS (
	SELECT
		_EXTRACT[1]::INTEGER AS ORDER_ID
		,_EXTRACT[2]::VARCHAR AS STATUS_NAME
		,_EXTRACT[3]::VARCHAR AS PRODUCT_NAME
		,_EXTRACT[4]::DATE AS CREATION_DATE
		,_EXTRACT[5]::DATE AS STATUS_CHANGE_DATE
		,CASE WHEN _EXTRACT[1]::INTEGER IN (SELECT * FROM ORDERS_PLACEMENT) THEN TRUE ELSE FALSE END AS IS_PLACEMENT
	FROM
		RAW_ORDERS
	WHERE
		TRUE
		AND
		_EXTRACT[1]::INTEGER NOT IN (SELECT * FROM TEST_ORDERS)
)

,PROV_RULES AS (
	SELECT
		(LOAD_RESULT ->> 'start')::DATE AS START_DATE
		,(LOAD_RESULT ->> 'end')::DATE AS END_DATE
		,(LOAD_RESULT ->> 'provision' ->> 'baseProvision')::FLOAT AS BASE_PROVISION
		,(LOAD_RESULT ->> 'provision' ->> 'placementProvision')::FLOAT AS PLACEMENT_PROVISION
		,(LOAD_RESULT -> 'provision' -> 'proportionalProvision' ->> 'target')::INTEGER AS PROP_PROV_TGT
		,(LOAD_RESULT -> 'provision' -> 'proportionalProvision' ->> 'value')::FLOAT AS PROP_PROV_VAL
		,MD5(CONCAT_WS('-',LOAD_RESULT ->> 'productName',LOAD_RESULT ->> 'start',LOAD_RESULT ->> 'end')) AS _CHECKSUM
		,LOAD_RESULT ->> 'productName' AS PRODUCT_NAME
	FROM
		{{ source('raw', 'provision_rules') }}
	QUALIFY
		ROW_NUMBER() OVER (PARTITION BY
			MD5(CONCAT_WS('-',LOAD_RESULT ->> 'productName',LOAD_RESULT ->> 'start',LOAD_RESULT ->> 'end'))
		ORDER BY 1) = 1
)

,PROV_RULES_ENRICHED AS (
	SELECT
		PROV_RULES.* EXCLUDE (PRODUCT_NAME)
		,MPRP.PRODUCT_NAME
	FROM
		PROV_RULES
	LEFT OUTER JOIN
		{{ ref('map_provision_rules_product') }} AS MPRP
		ON
			PROV_RULES.PRODUCT_NAME = MPRP.PROVISION_RULES_PRODUCT_NAME
)

,ORDERS_ENRICHED AS (
	SELECT
		ORDERS.*
		,PROV_RULES_ENRICHED.* EXCLUDE (_CHECKSUM,START_DATE,END_DATE,PRODUCT_NAME)
	FROM
		ORDERS
	LEFT OUTER JOIN
		PROV_RULES_ENRICHED
		ON
			ORDERS.PRODUCT_NAME = PROV_RULES_ENRICHED.PRODUCT_NAME
			AND
			ORDERS.CREATION_DATE BETWEEN PROV_RULES_ENRICHED.START_DATE AND PROV_RULES_ENRICHED.END_DATE
)

,ORDERS_ENRICHED_W_STATUS AS (
	SELECT
		*
		,MAX_BY(STATUS_NAME,STATUS_CHANGE_DATE) OVER (PARTITION BY ORDER_ID) AS CURRENT_STATUS
	FROM
		ORDERS_ENRICHED
)

,ORDERS_DEDUP AS (
	SELECT DISTINCT * EXCLUDE (STATUS_NAME,STATUS_CHANGE_DATE)
	FROM
		ORDERS_ENRICHED_W_STATUS
)

,CALC_BASE AS (
	SELECT
		*
		,COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH',CREATION_DATE),PRODUCT_NAME) AS GROSS_ORDER_PER_MONTH
		,COUNT(DISTINCT CASE WHEN CURRENT_STATUS = 'CONFIRMED' THEN ORDER_ID END)
			OVER (PARTITION BY DATE_TRUNC('MONTH',CREATION_DATE),PRODUCT_NAME)
			AS NET_ORDER_PER_MONTH
	FROM
		ORDERS_DEDUP
)

,CTE_GROSS AS (
	SELECT
		*
		,BASE_PROVISION AS GROSS_BASE_PROV
		,ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH',CREATION_DATE),PRODUCT_NAME ORDER BY CREATION_DATE,ORDER_ID) AS RN
		,CASE WHEN IS_PLACEMENT = TRUE THEN COALESCE(PLACEMENT_PROVISION,0) ELSE 0 END AS GROSS_PLC_PROV
		,CASE WHEN RN > PROP_PROV_TGT THEN PROP_PROV_VAL ELSE 0 END AS GROSS_PROP_PROV
	FROM
		CALC_BASE
)

,CTE_NET AS (
	SELECT
		*
		,BASE_PROVISION AS NET_BASE_PROV
		,ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH',CREATION_DATE),PRODUCT_NAME ORDER BY CREATION_DATE,ORDER_ID) AS RN
		,CASE WHEN IS_PLACEMENT = TRUE THEN COALESCE(PLACEMENT_PROVISION,0) ELSE 0 END AS NET_PLC_PROV
		,CASE WHEN RN > PROP_PROV_TGT THEN PROP_PROV_VAL ELSE 0 END AS NET_PROP_PROV
	FROM
		CALC_BASE
	WHERE
		TRUE
		AND
		CURRENT_STATUS = 'CONFIRMED'
)

,_RES AS (
	SELECT
		CTE_GROSS.ORDER_ID
		,CTE_GROSS.PRODUCT_NAME
		,CTE_GROSS.CREATION_DATE
		,CTE_GROSS.GROSS_BASE_PROV
		,CTE_GROSS.GROSS_PLC_PROV
		,CTE_GROSS.GROSS_PROP_PROV
		,COALESCE(CTE_NET.NET_BASE_PROV,0) AS NET_BASE_PROV
		,COALESCE(CTE_NET.NET_PLC_PROV,0) AS NET_PLC_PROV
		,COALESCE(CTE_NET.NET_PROP_PROV,0) AS NET_PROP_PROV
	FROM
		CTE_GROSS
	LEFT OUTER JOIN
		CTE_NET
		ON
			CTE_GROSS.ORDER_ID = CTE_NET.ORDER_ID
)

,_SQL_APPR AS (
	SELECT
		PRODUCT_NAME
		,DATE_TRUNC('MONTH',CREATION_DATE) AS CREATION_MONTH
		,SUM(GROSS_BASE_PROV) AS GROSS_BASE_PROV
		,SUM(GROSS_PLC_PROV) AS GROSS_PLC_PROV
		,SUM(GROSS_PROP_PROV) AS GROSS_PROP_PROV
		,SUM(NET_BASE_PROV) AS NET_BASE_PROV
		,SUM(NET_PLC_PROV) AS NET_PLC_PROV
		,SUM(NET_PROP_PROV) AS NET_PROP_PROV
	FROM
		_RES
	GROUP BY ALL
)

,_DBT_APPR AS (
	SELECT
		PRODUCT_NAME
		,DATE_TRUNC('MONTH',CREATION_DATE) AS CREATION_MONTH
		,SUM(GROSS_BASE_PROVISION) AS GROSS_BASE_PROVISION
		,SUM(GROSS_PLACEMENT_PROVISION) AS GROSS_PLACEMENT_PROVISION
		,SUM(GROSS_PROPORTIONAL_PROVISION) AS GROSS_PROPORTIONAL_PROVISION
		,SUM(NET_BASE_PROVISION) AS NET_BASE_PROVISION
		,SUM(NET_PLACEMENT_PROVISION) AS NET_PLACEMENT_PROVISION
		,SUM(NET_PROPORTIONAL_PROVISION) AS NET_PROPORTIONAL_PROVISION
	FROM {{ ref('mrt_orders') }}
	GROUP BY ALL
)

,SQL_MIN_DBT AS (
	SELECT * FROM _SQL_APPR
	EXCEPT
	SELECT * FROM _DBT_APPR
)

,DBT_MIN_SQL AS (
	SELECT * FROM _DBT_APPR
	EXCEPT
	SELECT * FROM _SQL_APPR
)

SELECT * FROM SQL_MIN_DBT
UNION ALL
SELECT * FROM DBT_MIN_SQL
