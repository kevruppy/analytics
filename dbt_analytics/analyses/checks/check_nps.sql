--noqa: disable=AM04,RF04,ST02

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
		CASE WHEN LIST_CONTAINS(ARRAY_AGG(DISTINCT IS_TEST_ORDER),TRUE) THEN TRUE ELSE FALSE END = TRUE
)

-- NPS
,NPS_PREP AS (
	SELECT
		*
		,SPLIT_PART(TRANSACTION_ID,'-',1) AS PRODUCT_ABBREVIATION
	FROM
		{{ source('raw', 'net_promotor_scores') }}
	WHERE
		TRUE
		AND
		RATING BETWEEN 0 AND 10
		AND
		SPLIT_PART(TRANSACTION_ID,'-',-1)::INTEGER NOT IN (SELECT TOR.ORDER_ID FROM TEST_ORDERS AS TOR)
)

,NPS_BASE AS (
	SELECT
		MPH.PRODUCT_GROUP
		,MPH.PRODUCT_TYPE
		,MPH.PRODUCT_NAME
		,NPS.RATING AS RATING_VALUE
		,YEAR(NPS.RATING_DATE) AS SURVEY_CALENDAR_YEAR
		,MONTH(NPS.RATING_DATE) AS SURVEY_CALENDAR_MONTH
	FROM
		NPS_PREP AS NPS
	LEFT OUTER JOIN
		{{ ref('map_net_promotor_scores_product_abbreviations') }} AS ABB
		ON NPS.PRODUCT_ABBREVIATION = ABB.PRODUCT_ABBREVIATION
	LEFT OUTER JOIN
		{{ ref('map_product_hierarchy') }} AS MPH
		ON
			ABB.PRODUCT_NAME = MPH.PRODUCT_NAME
			AND
			NPS.RATING_DATE BETWEEN MPH.VALID_FROM AND MPH.VALID_TO
)

,_SQL_APPR AS (
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
		,'TOTAL' AS AGGREGATION_LEVEL
	FROM NPS_BASE
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
		,'PRODUCT_GROUP' AS AGGREGATION_LEVEL
	FROM NPS_BASE
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
		,'PRODUCT_TYPE' AS AGGREGATION_LEVEL
	FROM NPS_BASE
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
		,'PRODUCT_NAME' AS AGGREGATION_LEVEL
	FROM NPS_BASE
	GROUP BY ALL
)

,_DBT_APPR AS (
	SELECT
		SURVEY_CALENDAR_YEAR
		,SURVEY_CALENDAR_MONTH
		,PRODUCT_GROUP
		,PRODUCT_TYPE
		,PRODUCT_NAME
		,PARTICIPANTS
		,DETRACTORS
		,INDIFFERENTS
		,PROMOTORS
		,AGGREGATION_LEVEL
	FROM {{ ref('mrt_net_promotor_scores') }}
)

,SQL_MIN_DBT AS (
	SELECT *
	FROM _SQL_APPR
	EXCEPT
	SELECT *
	FROM _DBT_APPR
)

,DBT_MIN_SQL AS (
	SELECT *
	FROM _DBT_APPR
	EXCEPT
	SELECT *
	FROM _SQL_APPR
)

SELECT * FROM SQL_MIN_DBT
UNION ALL
SELECT * FROM DBT_MIN_SQL
