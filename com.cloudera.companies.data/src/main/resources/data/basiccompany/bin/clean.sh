#!/bin/bash

DIR_BASE=../../../../../../src/data/resources/basiccompany
if [ $# -gt 0 ]; then
  DIR_BASE=$1
fi

rm -rvf $DIR_BASE/warehouse/csv/*.csv
rm -rvf $DIR_BASE/warehouse/zip/*.zip