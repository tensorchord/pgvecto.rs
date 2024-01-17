#!/usr/bin/env bash
set -e

d=$(psql -U postgres -tAqX -c "SELECT CURRENT_SETTING('data_directory')")/pg_vectors/indexes

a=$(sudo ls -l $d | wc -l)

printf "entries = $a\n" 

psql -f $(dirname $0)/test.sql

sleep 1

b=$(sudo ls -l $d | wc -l)

printf "entries = $b\n" 

if [ "$a" == "$b" ]; then
    echo "Transaction test [OK]"
else
    echo "Transaction test [FAILED]"
    exit 1
fi
