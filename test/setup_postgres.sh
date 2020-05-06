#!/bin/sh
BIN_PATH="$1"
DATA_PATH="$2"
if [ -z "$BIN_PATH" ]; then
    echo "$0 <BIN_PATH> <DATA_PATH>"
    exit 1
fi
export PATH="$BIN_PATH:$PATH"

set -ex

initdb "$DATA_PATH"

echo "wal_level = logical" >> $DATA_PATH/postgresql.conf

postgres -D  "$DATA_PATH" &
sleep 1

createdb -h localhost -U postgres test

