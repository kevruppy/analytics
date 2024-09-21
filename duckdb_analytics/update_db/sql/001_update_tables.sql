-- sqlfluff:rules:references.keywords:ignore_words:HEADER,JSON

/*
CREATE SECRET TO ALLOW IMPORTING DATA FROM S3 BUCKET
*/

-- noqa: disable=all

CREATE OR REPLACE SECRET AWS_SECRET (
    TYPE S3
	,KEY_ID 'KEY_ID__VALUE'
	,SECRET 'SECRET__VALUE'
	,REGION 'REGION__VALUE'
);

-- noqa: enable=all

/*
ORDERS
*/

-- LOAD CSV

CREATE OR REPLACE TEMPORARY TABLE TMP_ORDERS AS
SELECT COLUMNS(*)
FROM
	READ_CSV(
		's3://analytics-rawdata/update_orders.csv'
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
	TRUE
	AND
	(SELECT COUNT(*) > 0 AS _CHECK FROM RAW_DATA.ORDERS)
	AND
	NOT EXISTS
	-- noqa: disable=RF03
	(
		SELECT 1 AS COL
		FROM RAW_DATA.ORDERS AS TGT
		WHERE
			TRUE
			AND
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
		's3://analytics-rawdata/update_nps.json'
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
	TRUE
	AND
	(SELECT COUNT(*) > 0 AS _CHECK FROM RAW_DATA.NET_PROMOTOR_SCORES)
	AND
	(LOAD_RESULT ->> 'transaction_id')::VARCHAR NOT IN
		(
			SELECT TRANSACTION_ID
			FROM
				RAW_DATA.NET_PROMOTOR_SCORES
		);

DROP TABLE IF EXISTS TMP_NPS;

/*
PARTNERS
*/

TRUNCATE RAW_DATA.PARTNERS;

INSERT INTO RAW_DATA.PARTNERS (UPDATED_ON,PRODUCT_NAME,PARTNER_NAME)
SELECT
	UPDATED_ON
	,PRODUCT_NAME
	,PARTNER_NAME
FROM
	's3://analytics-rawdata/update_partners.csv';
