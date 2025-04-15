#!/bin/bash

# Define your PostgreSQL details
DB_NAME="temp"
DB_USER="postgres"
DB_PASSWORD="password"
DATA_DIR="."  # Directory where your .tbl files are stored
PSQL="psql -U $DB_USER -d $DB_NAME"

# Use sudo to run commands as 'postgres' user
echo "Loading schema..."
sudo -u postgres $PSQL -f "$DATA_DIR/dss.ddl"

# Load data into each table
for file in "$DATA_DIR"/*.tbl; do
    # Extract table name from filename
    base=$(basename "$file")
    table="${base%.tbl}"

    echo "Loading data into table: $table from file: $file"
    
    sudo -u postgres $PSQL -c "\copy $table FROM '$file' WITH (FORMAT TEXT, DELIMITER '|');"
    
    if [ $? -eq 0 ]; then
        echo "Loaded $file into $table"
    else
        echo "Failed to load $file into $table"
    fi
done
