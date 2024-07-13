---------------------------------------------------------------------------------------------------
-- SET SEED
---------------------------------------------------------------------------------------------------

SELECT SETSEED(1);

---------------------------------------------------------------------------------------------------
-- GENERATE INTS FROM 1 TO 1000
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE INTS AS
WITH RECURSIVE _INT AS
(
SELECT 1 AS _INT
--
UNION ALL
--
SELECT
	_INT + 1
FROM
	_INT
WHERE
	_INT < 1000
)
SELECT
	_INT
FROM
	_INT;

---------------------------------------------------------------------------------------------------
-- CONCAT INTS 3 TIMES AS ORDER STATUS HISTORY SHOULD HAVE MAX. LEN OF 3
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE INTS_REP AS 
SELECT
	*
FROM
	INTS
--
UNION ALL 
--
SELECT
	*
FROM
	INTS
--
UNION ALL
--
SELECT
	*
FROM
	INTS;

---------------------------------------------------------------------------------------------------
-- ENRICH TABLE 'INTS_REP' TO RANDOMLY DELETE ROWS (MATERIALIZED IN NEW TABLE 'INTS_REP_PREP')
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE INTS_REP_PREP AS
WITH CTE AS
(
SELECT
	*
FROM
	INTS_REP
ORDER BY
	RANDOM()
)
SELECT
	*
	,ROW_NUMBER() OVER(ORDER BY 1) % 2 = 0 AS _DELETION_MARKER
FROM
	CTE;

---------------------------------------------------------------------------------------------------
-- RANDOMLY DELETE ROWS FROM TABLE 'INTS_REP_PREP'
---------------------------------------------------------------------------------------------------

DELETE FROM INTS_REP_PREP WHERE _DELETION_MARKER = TRUE;

---------------------------------------------------------------------------------------------------
-- CHECK THE DISTRIBUTION
-- 3 IS OCCURING NOT OFTEN ENOUGH --> OVER-SAMPLE IN NEXT STEP
---------------------------------------------------------------------------------------------------

WITH CTE AS
(
SELECT
	_INT
	,COUNT(*) AS CNT
FROM
	INTS_REP_PREP
GROUP BY ALL
)
SELECT
	CNT
	,COUNT(*)
FROM
	CTE
GROUP BY ALL
ORDER BY
	CNT;

---------------------------------------------------------------------------------------------------
-- OVERSAMPLING...
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE INTS_OVERSAMPLE AS 
(
SELECT
	_INT
FROM
	INTS_REP_PREP
QUALIFY
	COUNT(*) OVER(PARTITION BY _INT) = 1
LIMIT
	50
)
--
UNION ALL
--
(
SELECT
	_INT
FROM
	INTS_REP_PREP
QUALIFY
	COUNT(*) OVER(PARTITION BY _INT) = 2
LIMIT
	150
)
--
UNION ALL
--
SELECT
	_INT
FROM
	INTS_REP_PREP
QUALIFY
	COUNT(*) OVER(PARTITION BY _INT) = 3
--
UNION ALL
--
SELECT
	_INT + 1000
FROM
	INTS_REP_PREP
QUALIFY
	COUNT(*) OVER(PARTITION BY _INT) = 3;

---------------------------------------------------------------------------------------------------
-- CREATE MORE ROWS TO FINALLY CREATE TABLE 'ORDER_ID_TBL'
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ORDER_ID_TBL AS
WITH CTE AS
(
SELECT * FROM INTS_OVERSAMPLE
--
UNION ALL
--
SELECT _INT + 2000 FROM INTS_OVERSAMPLE
--
UNION ALL
--
SELECT _INT + 4000 FROM INTS_OVERSAMPLE
--
UNION ALL
--
SELECT _INT + 6000 FROM INTS_OVERSAMPLE
--
UNION ALL
--
SELECT _INT + 8000 FROM INTS_OVERSAMPLE
)
SELECT
	*
FROM
	CTE
--
UNION ALL
--
SELECT
	_INT + 10000
FROM
	CTE;

---------------------------------------------------------------------------------------------------
-- CREATE TABLE 'ORDER_BASE'
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ORDER_BASE AS
SELECT
    _INT AS ORDER_ID
    -- ROW_NUMBERS SHOULD REFLECT STATUS HISTORY OF AN ORDER
    ,ROW_NUMBER() OVER(PARTITION BY _INT ORDER BY 1) AS RN
    -- RANDOM INTS ARE USED TO ASSIGN STATUSES
    ,FLOOR(1 + RANDOM() * 3) AS FIRST_RAND_INT
    ,FLOOR(1 + RANDOM() * 2) AS SECOND_RAND_INT
FROM
    ORDER_ID_TBL;

---------------------------------------------------------------------------------------------------
-- CREATE TABLE WITH FIRST & SECOND STATUS OF AN ORDER 'ORDER_FIRST_SECOND_STATUS'
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ORDER_FIRST_SECOND_STATUS AS
SELECT
    *
    -- INITIAL STATUS CAN ONLY BE 'CREATED'
    ,CASE
        WHEN RN = 1
            THEN 'CREATED'
    END AS INITIAL_STATUS
    -- SECOND STATUS CAN BE 'IN_PROGRES', 'CONFIRMED' OR 'CANCELLED'
    ,CASE
        WHEN RN = 2
            THEN
                CASE
                    WHEN FIRST_RAND_INT = 1
                        THEN 'IN_PROGRESS'
                    WHEN FIRST_RAND_INT = 2
                        THEN 'CONFIRMED'
                    ELSE 'CANCELLED'
                END
    END AS SECOND_STATUS
FROM
    ORDER_BASE;

---------------------------------------------------------------------------------------------------
-- CREATE TABLE WITH THIRD & FINAL STATUS OF AN ORDER 'ORDER_STATUS'
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ORDER_STATUS AS
SELECT
    ORDER_ID
    ,RN
    ,COALESCE(INITIAL_STATUS
            ,SECOND_STATUS
            ,CASE
                WHEN RN = 3 AND LAG(SECOND_STATUS) OVER(PARTITION BY ORDER_ID ORDER BY RN ASC) = 'IN_PROGRESS'
                    THEN
                        CASE
                            WHEN SECOND_RAND_INT = 1
                                THEN 'CANCELLED'
                            WHEN SECOND_RAND_INT = 2
                                THEN 'CONFIRMED'
                        END
            END) AS STATUS_NAME
FROM
    ORDER_FIRST_SECOND_STATUS;

---------------------------------------------------------------------------------------------------
-- INTRODUCE PRODUCT & DATE INFO FOR FINAL TABLE 'ORDER_TBL'
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ORDER_TBL AS
WITH CTE_RAND_INT AS
(
SELECT
    *
    ,FIRST_VALUE(FLOOR(1 + RANDOM() * 7)) OVER(PARTITION BY ORDER_ID) AS RAND_INT
FROM
    ORDER_STATUS
WHERE
    STATUS_NAME IS NOT NULL
),
CTE_ENRICH AS
(
SELECT
    *
    ,CASE
        WHEN RAND_INT = 1
            THEN '.COM'
        WHEN RAND_INT = 2
            THEN 'DSL'
        WHEN RAND_INT = 3
            THEN 'MWP'
        WHEN RAND_INT = 4
            THEN '.INFO'
        WHEN RAND_INT = 5
            THEN '.ONLINE'
        WHEN RAND_INT = 6
            THEN 'GLASS_FIBER'
        WHEN RAND_INT = 7
            THEN 'CLASSIC_HOSTING'
    END AS PRODUCT_NAME
    ,CASE
        WHEN RAND_INT < 4
            THEN '2022'
        ELSE '2023'
    END AS CREATION_YEAR
    ,FLOOR(1 + RANDOM() * 12)::INTEGER::VARCHAR AS CREATION_MONTH
FROM
    CTE_RAND_INT
),
CTE_CREATION_DATES AS
(
SELECT
    * EXCLUDE(RN, RAND_INT, CREATION_YEAR, CREATION_MONTH)
    ,COALESCE(
    	TRY_CAST(
    		CONCAT(CREATION_YEAR, '-', CREATION_MONTH, '-', LPAD(FLOOR(1 + RANDOM() * 31)::INTEGER::VARCHAR, 2, '0')) AS DATE)
        	,CONCAT(CREATION_YEAR, '-', CREATION_MONTH, '-01')::DATE) AS CREATION_DATE
FROM
    CTE_ENRICH
)
SELECT
	* EXCLUDE(CREATION_DATE)
	,FIRST_VALUE(CREATION_DATE) OVER(PARTITION BY ORDER_ID ORDER BY CREATION_DATE) AS CREATION_DATE
FROM
	CTE_CREATION_DATES;

---------------------------------------------------------------------------------------------------
-- SANITY CHECKS
---------------------------------------------------------------------------------------------------
-- 7.672

SELECT COUNT(*) FROM ORDER_TBL;

WITH CTE AS
(
SELECT
	ORDER_ID
	,COUNT(*) AS CNT
FROM
	ORDER_TBL
GROUP BY ALL
)
SELECT
	CNT
	,COUNT(*)
FROM
	CTE
GROUP BY ALL;

SELECT
	MIN(CREATION_DATE)
	,MAX(CREATION_DATE)
FROM
	ORDER_TBL;

SELECT
	ORDER_ID
	,COUNT(DISTINCT PRODUCT_NAME) AS CNT
FROM
	ORDER_TBL
GROUP BY ALL
ORDER BY
	CNT DESC;
	
SELECT
	PRODUCT_NAME
	,COUNT(DISTINCT ORDER_ID)
FROM
	ORDER_TBL 
GROUP BY ALL;

SELECT
	ORDER_ID
	,COUNT(DISTINCT CREATION_DATE) AS CNT
FROM
	ORDER_TBL
GROUP BY ALL
ORDER BY
	CNT DESC;

-- TODO:
-- ADD STATUS_CHANGE_DATE
	--> DO THIS BY CREATING A STATUS_ID AT THE TOP OF THE SCRIPT
	--> THEN USE A WINDOW FUNCTION & SOME RANDOM()
--> ONCE THIS IS DONE:
	--> CREATED HARCODED TABLE WITH COMM RULES
	--> ADD SOME TABLES FOR DIMENSIONS (E.G. WEB TRAFFIC, PRODUCT_INFO, NPS, CALCS?!)
	--> ONCE ALL FINISHED: ADD SOME NOISE TO THE DATA TO BE CLEANED UP USING DBT
	--> ADD DBT SEEDS LIKE 
		-- PRODUCTS:
		-- PRODUCT_LEVEL_1: PRIVATE_INTERNET | DOMAINS & HOSTING
		-- PRODUCT_LEVEL_2: DSL | GLASS_FIBER | DOMAINS | HOSTING 
		-- PRODUCT_LEVEL_3: DSL | GLASS_FIBER | .COM | .ONLINE | .INFO | CLASSIC HOSTING | MWP

SELECT
	*
	,CREATION_DATE + 1 AS STATUS_CHANGE_DATE
FROM
	ORDER_TBL
QUALIFY
	COUNT(*) OVER(PARTITION BY ORDER_ID) = 3
ORDER BY
	ORDER_ID;
