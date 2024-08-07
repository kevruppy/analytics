import duckdb
import pandas as pd

con = duckdb.connect("/workspaces/analytics/analytics.duckdb")

qry_splits = """WITH TMP AS
(
SELECT
	DATE_TRUNC('MONTH', CREATION_DATE) AS CREATION_MONTH
	,PRODUCT_NAME
	,COUNT(*) AS CNT 
FROM
	MARTS.MRT_ORDERS
GROUP BY ALL
)
,_NEEDED AS
(
WITH CTE_1 AS (SELECT DISTINCT CREATION_MONTH FROM TMP),
CTE_2 AS (SELECT DISTINCT PRODUCT_NAME FROM TMP)
SELECT * FROM CTE_1 CROSS JOIN CTE_2
)
SELECT _NEEDED.*
FROM _NEEDED
LEFT JOIN TMP USING(CREATION_MONTH, PRODUCT_NAME)
WHERE TMP.CNT IS NULL 
ORDER BY 1,2;"""

splits = con.execute(qry_splits).df()
seq_start = con.execute("SELECT MAX(ORDER_ID) AS X FROM STAGE.STG_ORDERS").df()["X"][0]
con.execute(f"CREATE OR REPLACE SEQUENCE SEQ_X START {seq_start}")


qry_template = """WITH CTE AS
(
SELECT ORDER_ID , STATUS_NAME, CREATION_DATE, STATUS_CHANGE_DATE, FALSE AS IS_TEST, NEXTVAL('SEQ_X') AS _NEW_ID
FROM
	STAGE.STG_ORDERS WHERE ORDER_ID NOT IN (SELECT DISTINCT ORDER_ID FROM INTERMEDIATE.INT_ORDERS_INVALID )
AND
	DATE_TRUNC('MONTH', CREATION_DATE) = '[MONTH]'
), XYZ AS
(
SELECT MIN(_NEW_ID) OVER (PARTITION BY ORDER_ID) AS NEW_ORDER_ID, STATUS_NAME, CREATION_DATE, STATUS_CHANGE_DATE, IS_TEST
FROM CTE)

SELECT NEW_ORDER_ID || ',' || STATUS_NAME || ',' || '[PRODUCT]' || ',"' || CREATION_DATE || '","' || STATUS_CHANGE_DATE || '",' || IS_TEST AS X
FROM XYZ;"""

df_list = []
for row in splits.iterrows():
    month = str(row[1][0])[:10]
    product = row[1][1]
    qry = qry_template
    x = qry.replace("[MONTH]", month).replace("[PRODUCT]", product)
    df_list.append(con.execute(x).df())


result_df = pd.concat(df_list)

result_df.to_csv("output.csv", sep=",", index=False, encoding="utf-8")
