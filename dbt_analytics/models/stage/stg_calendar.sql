SELECT
	CALENDAR_DATE
	,CALENDAR_WEEK
	,CALENDAR_MONTH
	,CALENDAR_QUARTER
	,CALENDAR_YEAR
	,CALENDAR_DAY_NAME
	,IS_WEEKEND
FROM
	{{ source('raw', 'calendar') }}
