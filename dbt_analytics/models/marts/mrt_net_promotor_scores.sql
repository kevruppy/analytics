-- NOTE: GROUPING SETS ARE NOT SUPPORTED BY SQLFLUFF
WITH CTE_AGG AS (
	SELECT
	-- DIMS
		SURVEY_CALENDAR_YEAR
		,SURVEY_CALENDAR_MONTH
		,'TOTAL' AS PRODUCT_GROUP
		,'TOTAL' AS PRODUCT_TYPE
		,'TOTAL' AS PRODUCT_NAME
		-- METRICS
		,COUNT(*) AS PARTICIPANTS
		,COUNT_IF(RATING_VALUE <= 6) AS DETRACTORS
		,COUNT_IF(RATING_VALUE IN (7,8)) AS INDIFFERENTS
		,COUNT_IF(RATING_VALUE >= 9) AS PROMOTORS
		,ROUND(DETRACTORS / PARTICIPANTS * 100,2) AS DETRACTORS_SHARE
		,ROUND(INDIFFERENTS / PARTICIPANTS * 100,2) AS INDIFFERENTS_SHARE
		,ROUND(PROMOTORS / PARTICIPANTS * 100,2) AS PROMOTORS_SHARE
		,ROUND(PROMOTORS_SHARE - DETRACTORS_SHARE,2) AS NET_PROMOTOR_SCORE
		,'TOTAL' AS AGGREGATION_LEVEL
	FROM {{ ref('int_net_promotor_scores') }}
	GROUP BY ALL
	UNION ALL
	SELECT
	-- DIMS
		SURVEY_CALENDAR_YEAR
		,SURVEY_CALENDAR_MONTH
		,PRODUCT_GROUP
		,'TOTAL' AS PRODUCT_TYPE
		,'TOTAL' AS PRODUCT_NAME
		-- METRICS
		,COUNT(*) AS PARTICIPANTS
		,COUNT_IF(RATING_VALUE <= 6) AS DETRACTORS
		,COUNT_IF(RATING_VALUE IN (7,8)) AS INDIFFERENTS
		,COUNT_IF(RATING_VALUE >= 9) AS PROMOTORS
		,ROUND(DETRACTORS / PARTICIPANTS * 100,2) AS DETRACTORS_SHARE
		,ROUND(INDIFFERENTS / PARTICIPANTS * 100,2) AS INDIFFERENTS_SHARE
		,ROUND(PROMOTORS / PARTICIPANTS * 100,2) AS PROMOTORS_SHARE
		,ROUND(PROMOTORS_SHARE - DETRACTORS_SHARE,2) AS NET_PROMOTOR_SCORE
		,'PRODUCT_GROUP' AS AGGREGATION_LEVEL
	FROM {{ ref('int_net_promotor_scores') }}
	GROUP BY ALL
	UNION ALL
	SELECT
	-- DIMS
		SURVEY_CALENDAR_YEAR
		,SURVEY_CALENDAR_MONTH
		,PRODUCT_GROUP
		,PRODUCT_TYPE
		,'TOTAL' AS PRODUCT_NAME
		-- METRICS
		,COUNT(*) AS PARTICIPANTS
		,COUNT_IF(RATING_VALUE <= 6) AS DETRACTORS
		,COUNT_IF(RATING_VALUE IN (7,8)) AS INDIFFERENTS
		,COUNT_IF(RATING_VALUE >= 9) AS PROMOTORS
		,ROUND(DETRACTORS / PARTICIPANTS * 100,2) AS DETRACTORS_SHARE
		,ROUND(INDIFFERENTS / PARTICIPANTS * 100,2) AS INDIFFERENTS_SHARE
		,ROUND(PROMOTORS / PARTICIPANTS * 100,2) AS PROMOTORS_SHARE
		,ROUND(PROMOTORS_SHARE - DETRACTORS_SHARE,2) AS NET_PROMOTOR_SCORE
		,'PRODUCT_TYPE' AS AGGREGATION_LEVEL
	FROM {{ ref('int_net_promotor_scores') }}
	GROUP BY ALL
	UNION ALL
	SELECT
	-- DIMS
		SURVEY_CALENDAR_YEAR
		,SURVEY_CALENDAR_MONTH
		,PRODUCT_GROUP
		,PRODUCT_TYPE
		,PRODUCT_NAME
		-- METRICS
		,COUNT(*) AS PARTICIPANTS
		,COUNT_IF(RATING_VALUE <= 6) AS DETRACTORS
		,COUNT_IF(RATING_VALUE IN (7,8)) AS INDIFFERENTS
		,COUNT_IF(RATING_VALUE >= 9) AS PROMOTORS
		,ROUND(DETRACTORS / PARTICIPANTS * 100,2) AS DETRACTORS_SHARE
		,ROUND(INDIFFERENTS / PARTICIPANTS * 100,2) AS INDIFFERENTS_SHARE
		,ROUND(PROMOTORS / PARTICIPANTS * 100,2) AS PROMOTORS_SHARE
		,ROUND(PROMOTORS_SHARE - DETRACTORS_SHARE,2) AS NET_PROMOTOR_SCORE
		,'PRODUCT_NAME' AS AGGREGATION_LEVEL
	FROM {{ ref('int_net_promotor_scores') }}
	GROUP BY ALL
)

SELECT
	-- DIMS
	SURVEY_CALENDAR_YEAR
	,SURVEY_CALENDAR_MONTH
	,PRODUCT_GROUP
	,PRODUCT_TYPE
	,PRODUCT_NAME
	-- METRICS
	,PARTICIPANTS::INTEGER AS PARTICIPANTS
	,DETRACTORS::INTEGER AS DETRACTORS
	,INDIFFERENTS::INTEGER AS INDIFFERENTS
	,PROMOTORS::INTEGER AS PROMOTORS
	,DETRACTORS_SHARE::FLOAT AS DETRACTORS_SHARE
	,INDIFFERENTS_SHARE::FLOAT AS INDIFFERENTS_SHARE
	,PROMOTORS_SHARE::FLOAT AS PROMOTORS_SHARE
	,NET_PROMOTOR_SCORE::FLOAT AS NET_PROMOTOR_SCORE
	,AGGREGATION_LEVEL
FROM
	CTE_AGG
