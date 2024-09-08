#!/bin/bash
export DB_PATH='/workspaces/analytics/analytics.duckdb'
source /workspaces/analytics/.venv/bin/activate
cd /workspaces/analytics/dbt_analytics
dbt parse --project-dir /workspaces/analytics/dbt_analytics
unset DB_PATH
