#!/bin/bash
set -e

POSTGRES="psql --username ${POSTGRES_USER}"

echo "Creating database role: roletest"

$POSTGRES <<-EOSQL
CREATE USER roletest;
ALTER USER roletest with supersuser;
EOSQL