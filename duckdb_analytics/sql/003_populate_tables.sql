-- sqlfluff:rules:references.keywords:ignore_words:HEADER,JSON

/*
ORDERS
*/

-- LOAD CSV

CREATE OR REPLACE TABLE TMP_ORDERS AS
SELECT COLUMNS(*)
FROM
	READ_CSV(
		'/workspaces/analytics/sample_data/orders.csv'
		,ALL_VARCHAR = TRUE
		,HEADER = TRUE
		,SEP = ','
	);

-- UNLOAD THE DATA AS JSON

COPY TMP_ORDERS
TO '~/TMP_DUCK_DB_EXPORT_ORDERS.json' (FORMAT JSON);

-- RE-LOAD THE JSON
--> ONLY JSON IN RAW_DATA SCHEMA

/*
PROVISION RULES
*/

INSERT INTO RAW_DATA.PROVISION_RULES (LOAD_RESULT)
SELECT JSON AS LOAD_RESULT
FROM
	READ_JSON(
		'/workspaces/analytics/sample_data/provision_rules.json'
		,RECORDS = FALSE
	);
