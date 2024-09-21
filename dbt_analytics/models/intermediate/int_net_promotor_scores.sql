WITH CTE_NPS_VALID AS (
	SELECT
		ORDER_ID
		,RATING_DATE
		,PRODUCT_NAME
		,RATING_VALUE
		,RATING_TOOL
	FROM
		{{ ref('cre_net_promotor_scores') }}
	WHERE
		TRUE
		AND
		IS_VALID_RATING = TRUE
)

,CTE_NPS_VALID_WO_TESTS AS (
	SELECT COLUMNS(*)
	FROM
		CTE_NPS_VALID
	WHERE
		TRUE
		AND
		ORDER_ID
		NOT IN (
			SELECT ORDER_ID FROM {{ ref('int_orders_invalid') }}
		)
)

,CTE_NPS_VALID_WO_TESTS_ENRICHED AS (
	SELECT
		-- NPS
		CTE_NPS_VALID_WO_TESTS.ORDER_ID
		,CTE_NPS_VALID_WO_TESTS.RATING_VALUE
		,CTE_NPS_VALID_WO_TESTS.RATING_TOOL
		-- CALENDAR
		,_CAL.CALENDAR_DATE AS SURVEY_CALENDAR_DATE
		,_CAL.CALENDAR_WEEK AS SURVEY_CALENDAR_WEEK
		,_CAL.CALENDAR_MONTH AS SURVEY_CALENDAR_MONTH
		,_CAL.CALENDAR_QUARTER AS SURVEY_CALENDAR_QUARTER
		,_CAL.CALENDAR_YEAR AS SURVEY_CALENDAR_YEAR
		,_CAL.CALENDAR_DAY_NAME AS SURVEY_CALENDAR_DAY_NAME
		,_CAL.IS_WEEKEND AS SURVEY_IS_WEEKEND
		-- PRODUCT_HIERARCHY
		,_MAP.PRODUCT_GROUP
		,_MAP.PRODUCT_TYPE
		,_MAP.PRODUCT_NAME
	FROM
		CTE_NPS_VALID_WO_TESTS
	INNER JOIN {{ ref('cre_calendar') }} AS _CAL
		ON CTE_NPS_VALID_WO_TESTS.RATING_DATE = _CAL.CALENDAR_DATE
	INNER JOIN {{ ref('map_product_hierarchy') }} AS _MAP
		ON
			CTE_NPS_VALID_WO_TESTS.PRODUCT_NAME = _MAP.PRODUCT_NAME
			AND CTE_NPS_VALID_WO_TESTS.RATING_DATE BETWEEN _MAP.VALID_FROM AND _MAP.VALID_TO
)

SELECT
	-- NPS
	ORDER_ID
	,RATING_VALUE
	,RATING_TOOL
	-- CALENDAR
	,SURVEY_CALENDAR_DATE
	,SURVEY_CALENDAR_WEEK
	,SURVEY_CALENDAR_MONTH
	,SURVEY_CALENDAR_QUARTER
	,SURVEY_CALENDAR_YEAR
	,SURVEY_CALENDAR_DAY_NAME
	,SURVEY_IS_WEEKEND
	-- PRODUCT_HIERARCHY
	,PRODUCT_GROUP
	,PRODUCT_TYPE
	,PRODUCT_NAME
FROM
	CTE_NPS_VALID_WO_TESTS_ENRICHED
