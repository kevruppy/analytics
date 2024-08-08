import duckdb
import gspread
import pandas as pd

gc = gspread.service_account("/workspaces/analytics/service_account.json")
sh = gc.open("MARTS")

sh.worksheet("MRT_ORDERS").clear()

con = duckdb.connect("/workspaces/analytics/analytics.duckdb")

qry = """
	SELECT
		PRODUCT_NAME
		,DATE_TRUNC('MONTH',CREATION_DATE) AS CREATION_MONTH
		,ROUND(SUM(GROSS_BASE_PROVISION),0) AS GROSS_BASE_PROVISION
		,ROUND(SUM(GROSS_PLACEMENT_PROVISION),0) AS GROSS_PLACEMENT_PROVISION
		,ROUND(SUM(GROSS_PROPORTIONAL_PROVISION),0) AS GROSS_PROPORTIONAL_PROVISION
		,ROUND(SUM(NET_BASE_PROVISION),0) AS NET_BASE_PROVISION
		,ROUND(SUM(NET_PLACEMENT_PROVISION),0) AS NET_PLACEMENT_PROVISION
		,ROUND(SUM(NET_PROPORTIONAL_PROVISION),0) AS NET_PROPORTIONAL_PROVISION
	FROM MARTS.MRT_ORDERS
	GROUP BY ALL
"""
result = con.execute(qry)

df = result.df()

worksheet = sh.worksheet("MRT_ORDERS")
worksheet.update([df.columns.values.tolist()] + df.values.tolist())
