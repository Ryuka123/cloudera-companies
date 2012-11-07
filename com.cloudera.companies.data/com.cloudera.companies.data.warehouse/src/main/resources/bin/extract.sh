#!/bin/sh

cd ../../data/zip

for FILE in *.zip; do
	if [ ! -f ../csv/${FILE%.*}".csv" ]; then
		unzip $FILE -d ../csv;
	fi
done