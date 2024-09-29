import duckdb

import streamlit as st

x = st.slider("Select a value")


token = st.secrets["MOTHERDUCK_TOKEN"]

CONNECTION_STRING = f"md:?motherduck_token={token}"
conn = duckdb.connect(CONNECTION_STRING)
y = st.dataframe(conn.sql("SELECT 1 AS X").df())
