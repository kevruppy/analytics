#!/bin/bash

git_status_output=$(git status --porcelain)

if [[ "$git_status_output" =~ "dbt_analytics/" ]]; then
  echo "### dbt related changes shown by 'git status' - dbt parsing... ###"

  ini_dir=$(pwd)
  export DB_PATH="/workspaces/analytics/analytics.duckdb"
  source /workspaces/analytics/.venv/bin/activate

  cd /workspaces/analytics/dbt_analytics
  dbt parse --project-dir /workspaces/analytics/dbt_analytics

  deactivate
  unset DB_PATH
  cd "$ini_dir"

  echo "### Finished dbt parsing ###"
else
  echo "### No dbt related changes found ###"
fi
