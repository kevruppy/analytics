WITH NET_PROMOTOR_SCORES AS (
	SELECT
		RATING_DATE
		,RATING AS RATING_VALUE
		,TOOL AS RATING_TOOL
		,SPLIT_PART(TRANSACTION_ID,'-',-1)::INTEGER AS ORDER_ID
		,SPLIT_PART(TRANSACTION_ID,'-',1) AS PRODUCT_ABBREVIATION
		,CASE WHEN RATING BETWEEN 0 AND 10 THEN TRUE ELSE FALSE END AS IS_VALID_RATING -- noqa: ST02
	FROM
		{{ ref('stage', 'stg_net_promotor_scores') }}
	{% if is_incremental() %}
		WHERE
			SPLIT_PART(TRANSACTION_ID,'-',-1) NOT IN (SELECT DISTINCT ORDER_ID FROM {{ this }})
	{% endif %}
)

SELECT
	_NPS.PRDER_ID
	,_MAP.PRODUCT_NAME
	,_NPS.RATING_DATE
	,_NPS.RATING_TOOL
	,_NPS.IS_VALID_RATING
FROM NET_PROMOTOR_SCORES AS _NPS
INNER JOIN {{ ref('mapping', '_net_promotor_scores_product_abbreviations_mapping') }} AS _MAP
	ON _NPS.PRODUCT_ABBREVIATION = _MAP.PRODUCT_ABBREVIATION
