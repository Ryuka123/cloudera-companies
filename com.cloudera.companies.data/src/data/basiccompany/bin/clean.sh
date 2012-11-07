#!/bin/bash

DIR_BASE=..
if [ $# -gt 0 ]; then
  DIR_BASE=$1
fi

rm -rvf $DIR_BASE/warehouse/csv/*.csv
rm -rvf $DIR_BASE/warehouse/zip/*.zip