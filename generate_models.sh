#!/bin/bash

sources=("wh_raw.trading_real_raw"
         "wh_raw.mobile_appsflyer"
         "wh_raw.users"
         "wh_raw.appsflyer_uninstall_events_report"
         "wh_raw.trading_otr_deals_real"
         "bloomreach.campaign")

# Ensure the models directory exists
mkdir -p models

for source in "${sources[@]}"; do
    IFS='.' read -ra ADDR <<< "$source"
    source_name="${ADDR[0]}"
    table_name="${ADDR[1]}"
    output_file="models/${source_name}_${table_name}_base_model.sql"

    # Run dbt command and redirect its output to the file
    dbt run-operation generate_base_model --args "{\"source_name\": \"${source_name}\", \"table_name\": \"${table_name}\"}" --project-dir . > "$output_file"
done
