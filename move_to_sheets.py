import duckdb
import gspread
import pandas as pd

gc = gspread.service_account("/workspaces/analytics/service_account.json")
sh = gc.open("MARTS")


for ws in sh.worksheets():
    ws.clear()

con = duckdb.connect("/workspaces/analytics/analytics.duckdb")

result = con.execute("SELECT ORDER_ID, PRODUCT_NAME FROM ANALYTICS.MARTS.MRT_ORDERS")

df = result.df()

worksheet = sh.worksheet("MRT_ORDERS")
worksheet.update([df.columns.values.tolist()] + df.values.tolist())
