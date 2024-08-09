import duckdb
import gspread
import pandas as pd

gc = gspread.service_account("/workspaces/analytics/service_account.json")
sh = gc.open("MARTS")

sh.worksheet("INPUT").clear()

con = duckdb.connect("/workspaces/analytics/analytics.duckdb")

qry = """
	SELECT
		PRODUCT_NAME
		,LEFT(REPLACE(DATE_TRUNC('MONTH',CREATION_DATE)::VARCHAR, '-', ''), 6) AS CREATION_MONTH
		,SUM(GROSS_BASE_PROVISION)::INTEGER AS GROSS_BASE_PROVISION
		,SUM(GROSS_PLACEMENT_PROVISION)::INTEGER AS GROSS_PLACEMENT_PROVISION
		,SUM(GROSS_PROPORTIONAL_PROVISION)::INTEGER AS GROSS_PROPORTIONAL_PROVISION
		,SUM(NET_BASE_PROVISION)::INTEGER AS NET_BASE_PROVISION
		,SUM(NET_PLACEMENT_PROVISION)::INTEGER AS NET_PLACEMENT_PROVISION
		,SUM(NET_PROPORTIONAL_PROVISION)::INTEGER AS NET_PROPORTIONAL_PROVISION
		,SUM(GROSS_BASE_PROVISION + GROSS_PLACEMENT_PROVISION + GROSS_PLACEMENT_PROVISION)::INTEGER AS GROSS_PROVISION
		,SUM(NET_BASE_PROVISION + NET_PLACEMENT_PROVISION + NET_PROPORTIONAL_PROVISION)::INTEGER AS NET_PROVISION
        ,COUNT(*) AS CNT
	FROM MARTS.MRT_ORDERS
	GROUP BY ALL
    ORDER BY CREATION_MONTH, PRODUCT_NAME
"""
result = con.execute(qry)

df = result.df()

worksheet = sh.worksheet("INPUT")
worksheet.update([df.columns.values.tolist()] + df.values.tolist())
