-- sqlfluff:rules:references.keywords:ignore_words:HEADER,JSON

/*
ORDERS
*/

-- LOAD CSV

CREATE OR REPLACE TEMPORARY TABLE TMP_ORDERS AS
SELECT COLUMNS(*)
FROM
	READ_CSV(
		'/workspaces/analytics/duckdb_analytics/update_db/sample_data/orders.csv'
		,ALL_VARCHAR = TRUE
		,HEADER = TRUE
		,SEP = ','
	);

INSERT INTO RAW_DATA.ORDERS (LOAD_RESULT)
SELECT
	{
		'ORDER_ID': ORDER_ID
		,'STATUS_NAME': STATUS_NAME
		,'PRODUCT_NAME': PRODUCT_NAME
		,'CREATION_DATE': CREATION_DATE
		,'STATUS_CHANGE_DATE': STATUS_CHANGE_DATE
		,'IS_TEST_ORDER': IS_TEST_ORDER
	}::JSON AS LOAD_RESULT
FROM
	TMP_ORDERS AS SRC
WHERE
	NOT EXISTS
	-- noqa: disable=RF03
	(
		SELECT 1 AS COL
		FROM RAW_DATA.ORDERS AS TGT
		WHERE
			(TGT.LOAD_RESULT ->> 'ORDER_ID')::VARCHAR = SRC.ORDER_ID
			AND (TGT.LOAD_RESULT ->> 'STATUS_NAME')::VARCHAR = SRC.STATUS_NAME
	);
-- noqa: enable=all

DROP TABLE IF EXISTS TMP_ORDERS;

/*
NET PROMOTOR SCORE (NPS)
*/

CREATE OR REPLACE TEMPORARY TABLE TMP_NPS AS
SELECT JSON AS LOAD_RESULT
FROM
	READ_JSON(
		'/workspaces/analytics/duckdb_analytics/update_db/sample_data/nps.json'
		,RECORDS = FALSE
	);

INSERT INTO RAW_DATA.NET_PROMOTOR_SCORES (TRANSACTION_ID,RATING_DATE,RATING,TOOL)
SELECT
	(LOAD_RESULT ->> 'transaction_id')::VARCHAR AS TRANSACTION_ID
	,(LOAD_RESULT ->> 'rating_date')::DATE AS RATING_DATE
	,(LOAD_RESULT ->> 'rating')::INTEGER AS RATING
	,(LOAD_RESULT ->> 'tool')::VARCHAR AS TOOL
FROM
	TMP_NPS
WHERE
	(LOAD_RESULT ->> 'transaction_id')::VARCHAR NOT IN (SELECT TRANSACTION_ID FROM RAW_DATA.NET_PROMOTOR_SCORES);

DROP TABLE IF EXISTS TMP_NPS;
