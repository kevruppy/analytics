-- sqlfluff:rules:references.keywords:ignore_words:HEADER,JSON

/*
ORDERS
*/

-- LOAD CSV

CREATE OR REPLACE TEMPORARY TABLE TMP_ORDERS AS
SELECT COLUMNS(*)
FROM
	READ_CSV(
		'/workspaces/analytics/duckdb_analytics/initialize_db/sample_data/orders.csv'
		,ALL_VARCHAR = TRUE
		,HEADER = TRUE
		,SEP = ','
	);

-- UNLOAD THE DATA AS JSON

COPY TMP_ORDERS
TO '~/TMP_DUCK_DB_EXPORT_ORDERS.json' (FORMAT JSON);

-- RE-LOAD THE JSON
--> MOSTLY JSON IN RAW_DATA SCHEMA

INSERT INTO RAW_DATA.ORDERS (LOAD_RESULT)
SELECT JSON AS LOAD_RESULT
FROM
	READ_JSON(
		'~/TMP_DUCK_DB_EXPORT_ORDERS.json'
		,RECORDS = FALSE
	);

DROP TABLE IF EXISTS TMP_ORDERS;

/*
ORDERS WITH PLACEMENT
*/

-- LOAD CSV

CREATE OR REPLACE TEMPORARY TABLE TMP_ORDERS_WITH_PLACEMENT AS
SELECT COLUMNS(*)
FROM
	READ_CSV(
		'/workspaces/analytics/duckdb_analytics/initialize_db/sample_data/orders_with_placement.csv'
		,ALL_VARCHAR = TRUE
		,HEADER = TRUE
		,SEP = ','
	);

-- UNLOAD THE DATA AS JSON

COPY TMP_ORDERS_WITH_PLACEMENT
TO '~/TMP_DUCK_DB_EXPORT_ORDERS_WITH_PLACEMENT.json' (FORMAT JSON);

-- RE-LOAD THE JSON
--> MOSTLY JSON IN RAW_DATA SCHEMA

INSERT INTO RAW_DATA.ORDERS_WITH_PLACEMENT (LOAD_RESULT)
SELECT JSON AS LOAD_RESULT
FROM
	READ_JSON(
		'~/TMP_DUCK_DB_EXPORT_ORDERS_WITH_PLACEMENT.json'
		,RECORDS = FALSE
	);

DROP TABLE IF EXISTS TMP_ORDERS_WITH_PLACEMENT;

/*
PROVISION RULES
*/

INSERT INTO RAW_DATA.PROVISION_RULES (LOAD_RESULT)
SELECT JSON AS LOAD_RESULT
FROM
	READ_JSON(
		'/workspaces/analytics/duckdb_analytics/initialize_db/sample_data/provision_rules.json'
		,RECORDS = FALSE
	);

/*
NET PROMOTOR SCORE (NPS)
*/

INSERT INTO RAW_DATA.NET_PROMOTOR_SCORES (TRANSACTION_ID,RATING_DATE,RATING,TOOL)
SELECT
	TRANSACTION_ID
	,RATING_DATE
	,RATING
	,TOOL
FROM
	'/workspaces/analytics/duckdb_analytics/initialize_db/sample_data/nps.csv'; --noqa

/*
EXCHANGE RATES
*/

INSERT INTO RAW_DATA.EXCHANGE_RATES (LOAD_RESULT)
SELECT JSON AS LOAD_RESULT
FROM
	READ_JSON(
		'/workspaces/analytics/duckdb_analytics/initialize_db/sample_data/exchange_rates.json'
		,RECORDS = FALSE
	);
