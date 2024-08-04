import duckdb
import gspread
import pandas as pd

gc = gspread.service_account("/workspaces/analytics/service_account.json")
sh = gc.open("MARTS")


for ws in sh.worksheets():
    ws.clear()

con = duckdb.connect("/workspaces/analytics/analytics.duckdb")

qry = """
SELECT
	ORDER_ID
	,PRODUCT_NAME
	,PRODUCT_TYPE
	,PRODUCT_GROUP
	,IS_PLACEMENT
	,CURRENT_STATUS
	,COALESCE(CREATION_DATE::STRING, 'X') AS CREATION_DATE
	,COALESCE(FORWARDING_DATE::STRING, 'X') AS FORWARDING_DATE
	,COALESCE(CONFIRMATION_DATE::STRING, 'X') AS CONFIRMATION_DATE
	,COALESCE(CANCELLATION_DATE::STRING, 'X') AS CANCELLATION_DATE
	,IS_FORWARDED
	,IS_CANCELLED
	,IS_CONFIRMED
	,GROSS_BASE_PROVISION::STRING AS GROSS_BASE_PROV
	,NET_BASE_PROVISION::STRING AS NET_BASE_PROV
	,GROSS_PLACEMENT_PROVISION::STRING AS GROSS_PLC_PROV
	,NET_PLACEMENT_PROVISION::STRING AS NET_PLC_PROV
	,GROSS_PROPORTIONAL_PROVISION::STRING AS GROSS_PROP_PROV
	,NET_PROPORTIONAL_PROVISION::STRING AS NET_PROP_PROV
FROM
	ANALYTICS.MARTS.MRT_ORDERS
"""
result = con.execute(qry)

df = result.df()

worksheet = sh.worksheet("MRT_ORDERS")
worksheet.update([df.columns.values.tolist()] + df.values.tolist())
