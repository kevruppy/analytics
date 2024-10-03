WITH CTE AS (
	SELECT
		TRANSACTION_ID
		,RATING_DATE
		,RATING
		,TOOL
	FROM
		{{ source('raw', 'net_promotor_scores') }}
	{% if is_incremental() %}
		WHERE
			TRANSACTION_ID NOT IN (SELECT DISTINCT TGT.TRANSACTION_ID FROM {{ this }} AS TGT)
	{% endif %}
)

SELECT
	TRANSACTION_ID
	,RATING_DATE
	,RATING
	,TOOL
FROM
	CTE
