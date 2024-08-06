-- COUNTS PER PRODUCT & MONTH
CREATE OR REPLACE TEMP TABLE TMP AS 
SELECT
	DATE_TRUNC('MONTH', CREATION_DATE) AS CREATION_MONTH
	,PRODUCT_NAME
	,COUNT(*) AS CNT 
FROM
	MARTS.MRT_ORDERS
GROUP BY ALL;

-- NEEDED SPLITS
CREATE OR REPLACE TEMP TABLE _NEEDED AS 
WITH CTE_1 AS (SELECT DISTINCT CREATION_MONTH FROM TMP),
CTE_2 AS (SELECT DISTINCT PRODUCT_NAME FROM TMP)
SELECT * FROM CTE_1 CROSS JOIN CTE_2;

-- MISSING
SELECT _NEEDED.*
FROM _NEEDED
LEFT JOIN TMP USING(CREATION_MONTH, PRODUCT_NAME)
WHERE TMP.CNT IS NULL 
ORDER BY 1,2;


SELECT MAX(ORDER_ID) FROM STAGE.STG_ORDERS;

CREATE OR REPLACE SEQUENCE SEQ_X START 69999;

CREATE OR REPLACE TEMP TABLE XYZ AS
WITH CTE AS
(
SELECT ORDER_ID , STATUS_NAME, CREATION_DATE, STATUS_CHANGE_DATE, FALSE AS IS_TEST, NEXTVAL('SEQ_X') AS _NEW_ID
FROM
	STAGE.STG_ORDERS WHERE ORDER_ID NOT IN (SELECT DISTINCT ORDER_ID FROM INTERMEDIATE.INT_ORDERS_INVALID )
AND
	DATE_TRUNC('MONTH', CREATION_DATE) = '2022-01-01' 
)
SELECT MIN(_NEW_ID) OVER (PARTITION BY ORDER_ID) AS NEW_ORDER_ID, STATUS_NAME, CREATION_DATE, STATUS_CHANGE_DATE, IS_TEST
FROM CTE;

SELECT NEW_ORDER_ID || ',' || STATUS_NAME || ',' || '.INFO' || ',"' || CREATION_DATE || '","' || STATUS_CHANGE_DATE || '",' || IS_TEST AS X
FROM XYZ;