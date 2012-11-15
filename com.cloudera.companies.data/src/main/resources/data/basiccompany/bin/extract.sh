#!/bin/bash

DIR_BASE=../../../../../../src/data/resources/basiccompany
if [ $# -gt 0 ]; then
  DIR_BASE=$1
fi

cd $DIR_BASE/warehouse/zip
for FILE in *.zip; do
	if [ ! -f ../csv/${FILE%.*}".csv" ]; then
		unzip $FILE -d ../csv;
	fi
done