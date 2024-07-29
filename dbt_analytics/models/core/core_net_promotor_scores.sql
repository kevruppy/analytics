SELECT
	TRANSACTION_ID
	,RATING_DATE
	,RATING
	,TOOL
FROM
	{{ ref('core', 'net_promotor_scores') }}
