#!/bin/bash

git_status_output=$(git status --porcelain)

if [[ "$git_status_output" =~ "dbt_analytics/" ]]; then
    echo "### dbt related changes shown by 'git status' ###"

    ini_dir=$(pwd)
    source ./.venv/bin/activate

    cd ./dbt_analytics

    export DB_PATH="$(dirname "$PWD")/analytics.duckdb"

    echo "### dbt debug... ###"

    dbt_debug_log=".././zzz_dbt_debug.log"
    dbt debug > "$dbt_debug_log" 2>&1

    if [[ $? -ne 0 ]]; then
        deactivate
        unset DB_PATH
        cd "$ini_dir"
        echo "### ERROR: 'dbt debug' failed. Check $dbt_debug_log for details. ###"
        exit 1
    fi

    # Delete log if dbt debug completes without errors
    rm "$dbt_debug_log"
    deactivate
    unset DB_PATH
    cd "$ini_dir"

    echo "### SUCCESS ###"
else
    echo "### No dbt related changes shown by 'git status' ###"
fi
