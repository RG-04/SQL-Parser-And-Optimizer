#!/bin/bash

DB_NAME="temp"
DB_USER="postgres"
DATA_DIR="tpch-dbgen" # change accordingly
PSQL="psql -U $DB_USER -d $DB_NAME"

${PSQL} -f "$DATA_DIR/dss.ddl"

for file in "$DATA_DIR"/*.tbl; do
    # Extract table name from filename
    base=$(basename "$file")
    table="${base%.tbl}"

    echo "�� Loading data into table: $table from file: $file"
    
    $PSQL -c "\copy $table FROM '$file' WITH (FORMAT csv, DELIMITER '|', NULL '', HEADER false)"
    
    if [ $? -eq 0 ]; then
        echo "Loaded $file into $table"
    else
        echo "Failed to load $file into $table"
    fi
done

