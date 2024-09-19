#!/bin/bash

git_status_output=$(git status --porcelain)

if [[ "$git_status_output" =~ "dbt_analytics/" ]]; then
  echo "### dbt related changes shown by 'git status' ###"

  ini_dir=$(pwd)
  source ./.venv/bin/activate

  cd ./dbt_analytics

  export DB_PATH="$(dirname "$PWD")/analytics.duckdb"

  ### dbt parse

  echo "### dbt parse... ###"

  dbt_parse_log=".././zzz_dbt_parse.log"
  dbt parse > "$dbt_parse_log" 2>&1

  if [[ $? -ne 0 ]]; then
      deactivate
      unset DB_PATH
      cd "$ini_dir"
      echo "### ERROR: 'dbt parse' failed. Check $dbt_parse_log for details. ###"
      exit 1
  fi

  rm "$dbt_parse_log"

  # dbt docs generate

  echo "### dbt docs generate... ###"

  dbt_docs_generate_log=".././zzz_dbt_docs_generate.log"
  dbt docs generate > "$dbt_docs_generate_log" 2>&1

  if [[ $? -ne 0 ]]; then
      deactivate
      unset DB_PATH
      cd "$ini_dir"
      echo "### ERROR: 'dbt docs generate' failed. Check $dbt_docs_generate_log for details. ###"
      exit 1
  fi

  rm "$dbt_docs_generate_log"

  deactivate
  unset DB_PATH
  cd "$ini_dir"

  echo "### SUCCESS ###"
else
  echo "### No dbt related changes shown by 'git status' ###"
fi
