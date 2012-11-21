#!/bin/bash

DIR_BASE=..
if [ $# -gt 0 ]; then
  DIR_BASE=$1
fi

rm -rvf $DIR_BASE/sample/zip/*.zip
find $DIR_BASE/sample/csv -name "*.csv" -exec zip {}.zip {} \;
mv -v $DIR_BASE/sample/csv/*.zip $DIR_BASE/sample/zip 
for i in $DIR_BASE/sample/zip/*.zip; do j=`echo $i | sed 's/.csv././g'`; mv "$i" "$j"; done