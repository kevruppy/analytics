-- ORDERS

-- LOAD CSV

CREATE OR REPLACE TABLE TMP_ORDERS AS
SELECT * FROM READ_CSV('/workspaces/analytics/sample_data/orders.csv'
,all_varchar = True
,header = TRUE
,sep = ',');

-- UNLOAD THE DATA AS JSON

COPY TMP_ORDERS TO 'TMP_ORDERS.json' (FORMAT JSON);

-- RE-LOAD THE JSON
--> ONLY JSON IN RAW_DATA SCHEMA

-- PROVISION RULES

INSERT INTO RAW_DATA.PROVISION_RULES (LOAD_RESULT)
SELECT JSON AS LOAD_RESULT --noqa: RF04
FROM
	READ_JSON(
		'/workspaces/analytics/sample_data/provision_rules.json'
		,RECORDS = FALSE
	);
