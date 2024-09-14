#!/bin/bash

git_status_output=$(git status --porcelain)

if [[ "$git_status_output" =~ "dbt_analytics/" ]]; then
  echo "### dbt related changes shown by 'git status' ###"

  ini_dir=$(pwd)
  export DB_PATH="/workspaces/analytics/analytics.duckdb"
  source /workspaces/analytics/.venv/bin/activate

  cd /workspaces/analytics/dbt_analytics

  echo "### dbt parse... ###"
  dbt parse --project-dir /workspaces/analytics/dbt_analytics

  echo "### dbt docs generate... ###"
  dbt docs generate --project-dir /workspaces/analytics/dbt_analytics

  deactivate
  unset DB_PATH
  cd "$ini_dir"

  echo "### SUCCESS ###"
else
  echo "### No dbt related changes shown by 'git status' ###"
fi
