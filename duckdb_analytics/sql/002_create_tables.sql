/*
ORDERS
*/

CREATE SEQUENCE IF NOT EXISTS RAW_DATA.ORDERS_SEQ START 1;

CREATE TABLE IF NOT EXISTS RAW_DATA.ORDERS
(
	_ID INTEGER NOT NULL DEFAULT NEXTVAL('RAW_DATA.ORDERS_SEQ')
	,_ADDED_ON TIMESTAMP DEFAULT GET_CURRENT_TIMESTAMP()
	,LOAD_RESULT JSON NOT NULL
);

COMMENT ON TABLE RAW_DATA.ORDERS IS
'CONTAINS RAWDATA FOR ORDERS (APPEND ONLY)';

COMMENT ON COLUMN RAW_DATA.ORDERS._ID IS
'INCREMENTAL AUTO-ID USING SEQUENCE';

COMMENT ON COLUMN RAW_DATA.ORDERS._ADDED_ON IS
'TIMESTAMP WHEN RECORD WAS ADDED';

COMMENT ON COLUMN RAW_DATA.ORDERS.LOAD_RESULT IS
'RAW RESULT OF LOAD';

/*
ORDERS WITH PLACEMENT
*/

CREATE SEQUENCE IF NOT EXISTS RAW_DATA.ORDERS_WITH_PLACEMENT_SEQ START 1;

CREATE TABLE IF NOT EXISTS RAW_DATA.ORDERS_WITH_PLACEMENT
(
	_ID INTEGER NOT NULL DEFAULT NEXTVAL('RAW_DATA.ORDERS_WITH_PLACEMENT_SEQ')
	,_ADDED_ON TIMESTAMP DEFAULT GET_CURRENT_TIMESTAMP()
	,LOAD_RESULT JSON NOT NULL
);

COMMENT ON TABLE RAW_DATA.ORDERS_WITH_PLACEMENT IS
'CONTAINS RAWDATA FOR ORDERS WITH PLACEMENT, INDICATES THAT ORDER IS WITH PLACEMENT (APPEND ONLY)';

/*
PROVISION RULES
*/

CREATE SEQUENCE IF NOT EXISTS RAW_DATA.PROVISION_RULES_SEQ START 1;

CREATE TABLE IF NOT EXISTS RAW_DATA.PROVISION_RULES
(
	_ID INTEGER NOT NULL DEFAULT NEXTVAL('RAW_DATA.PROVISION_RULES_SEQ')
	,_ADDED_ON TIMESTAMP DEFAULT GET_CURRENT_TIMESTAMP()
	,LOAD_RESULT JSON NOT NULL
);

COMMENT ON TABLE RAW_DATA.PROVISION_RULES IS
'CONTAINS RAWDATA FOR PROVISION RULES (APPEND ONLY)';

COMMENT ON COLUMN RAW_DATA.PROVISION_RULES._ID IS
'INCREMENTAL AUTO-ID USING SEQUENCE';

COMMENT ON COLUMN RAW_DATA.PROVISION_RULES._ADDED_ON IS
'TIMESTAMP WHEN RECORD WAS ADDED';

COMMENT ON COLUMN RAW_DATA.PROVISION_RULES.LOAD_RESULT IS
'RAW RESULT OF LOAD';

/*
NET PROMOTOR SCORES (NPS)
*/

CREATE SEQUENCE IF NOT EXISTS RAW_DATA.NET_PROMOTOR_SCORES_SEQ START 1;

CREATE TABLE IF NOT EXISTS RAW_DATA.NET_PROMOTOR_SCORES
(
	_ID INTEGER NOT NULL DEFAULT NEXTVAL('RAW_DATA.NET_PROMOTOR_SCORES_SEQ')
	,_ADDED_ON TIMESTAMP DEFAULT GET_CURRENT_TIMESTAMP()
	,TRANSACTION_ID STRING
	,RATING_DATE DATE
	,RATING INTEGER
	,TOOL STRING
);

COMMENT ON TABLE RAW_DATA.NET_PROMOTOR_SCORES IS
'CONTAINS RAWDATA FOR NET PROMOTOR SCORE (NPS) RATINGS (APPEND ONLY)';

COMMENT ON COLUMN RAW_DATA.NET_PROMOTOR_SCORES._ID IS
'INCREMENTAL AUTO-ID USING SEQUENCE';

COMMENT ON COLUMN RAW_DATA.NET_PROMOTOR_SCORES._ADDED_ON IS
'TIMESTAMP WHEN RECORD WAS ADDED';

COMMENT ON COLUMN RAW_DATA.NET_PROMOTOR_SCORES.TRANSACTION_ID IS
'IDENTIFIER OF ORDER RATED';

COMMENT ON COLUMN RAW_DATA.NET_PROMOTOR_SCORES.RATING_DATE IS
'DATE ON WHICH RATING WAS COLLECTED';

COMMENT ON COLUMN RAW_DATA.NET_PROMOTOR_SCORES.RATING IS
'RATING GIVEN BY CUSTOMER';

COMMENT ON COLUMN RAW_DATA.NET_PROMOTOR_SCORES.TOOL IS
'TOOL WITH WHICH THE RATING WAS COLLECTED';
