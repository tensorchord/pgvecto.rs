#!/usr/bin/env bash
set -e

cat << EOF
#include "postgres.h"

#include "access/amapi.h"
#include "fmgr.h"

#include <stdlib.h>

#define DECLARE(funcname)                                                      \
  extern Datum funcname() { exit(1); }                                         \
  extern const Pg_finfo_record *pg_finfo_##funcname(void);                     \
  const Pg_finfo_record *pg_finfo_##funcname(void) { return &my_finfo; }       \
  extern int no_such_variable

#define DECLARE_AMHANDLER(funcname)                                            \
  extern Datum funcname() { return amhandler(); }                              \
  extern const Pg_finfo_record *pg_finfo_##funcname(void);                     \
  const Pg_finfo_record *pg_finfo_##funcname(void) { return &my_finfo; }       \
  extern int no_such_variable

static const Pg_finfo_record my_finfo = {1};

PG_MODULE_MAGIC;

Datum amhandler() {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
  amroutine->amcanorderbyop = true;
  (Datum) amroutine;
}
EOF

printf "\n"

while read -r line; do
    if [[ $line == *"amhandler"* ]]; then
        echo "DECLARE_AMHANDLER($line);"
    else
        echo "DECLARE($line);"
    fi
done <<< $(grep -ohr "'\w\+_wrapper'" $(dirname "$0")/../sql | sort | uniq | sed "s/'//g")
