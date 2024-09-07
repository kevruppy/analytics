-- NOTE: THIS MODEL IS AN EXCEPTION AS IT NEITHER USES SOURCES NOR REFS

WITH CTE AS (
	SELECT -- noqa: ST06
		DATE_TRUNC('DAY',GENERATE_SERIES) AS CALENDAR_DATE
		,WEEK(CALENDAR_DATE)::INTEGER AS CALENDAR_WEEK
		,MONTH(CALENDAR_DATE)::INTEGER AS CALENDAR_MONTH
		,QUARTER(CALENDAR_DATE)::INTEGER AS CALENDAR_QUARTER
		,YEAR(CALENDAR_DATE)::INTEGER AS CALENDAR_YEAR
		,DAYNAME(CALENDAR_DATE) AS CALENDAR_DAY_NAME
		,CASE WHEN CALENDAR_DAY_NAME IN ('Saturday','Sunday') THEN TRUE ELSE FALSE END AS IS_WEEKEND -- noqa: ST02
	FROM
		GENERATE_SERIES('2020-01-01'::DATE,'2030-12-31'::DATE,INTERVAL 1 DAY)
)

SELECT
	CALENDAR_DATE
	,CALENDAR_WEEK
	,CALENDAR_MONTH
	,CALENDAR_QUARTER
	,CALENDAR_YEAR
	,CALENDAR_DAY_NAME
	,IS_WEEKEND
FROM
	CTE
